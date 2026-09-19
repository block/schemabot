package serve

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"time"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/ddl"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/tern"
)

// storageSchemaAdapter answers the storage-schema RPCs for the instance it was
// built on: it reads that instance's own storage database, with that
// instance's own embedded schema files, and converges it by running that
// instance's own startup bootstrap.
//
// The bindings are what make the answer trustworthy, so they are fixed at
// construction and nothing on the wire can move them. There is no target in
// the request, so no caller — not even the control plane — can point this at a
// different database.
//
// Two things a caller may influence, and neither reaches the database a
// convergence writes to. Whether destructive statements run only ever widens
// what the local config already allows, and on a locally hosted server not even
// that (see destructivePolicy). A
// desired schema sent with a diff replaces the files the comparison reads, and
// is accepted only there: the convergence RPC carries no schema, so an apply
// always runs this binary's own (see desiredSchema).
type storageSchemaAdapter struct {
	// resolveDSN re-resolves the storage DSN per call rather than capturing a
	// string, so a credential rotated since startup is picked up the same way
	// the storage pool picks it up.
	resolveDSN func() (string, error)
	// bootTarget is the database this instance booted against, parsed out of
	// the DSN its storage pool was opened with. Every re-resolved DSN is
	// checked against it, so rotation moves the credentials and nothing moves
	// the database.
	bootTarget storageTarget
	dialect    schema.Dialect
	// version attributes a report's embedded schema files to the build that
	// carries them, and is empty when this build cannot be named (see
	// attributableVersion).
	version string
	// configAllowsDestructive is the deployment's standing storage policy
	// (storage.allow_destructive_schema_changes). A boot converges under it, so
	// an operator convergence must too — otherwise "apply is what a boot does"
	// would stop being true on exactly the deployments that opted in.
	configAllowsDestructive bool
	// localHosted marks a server the local runtime hosts, where no route to a
	// destructive storage bootstrap exists at all (AZ-6).
	localHosted bool
	// postgresStatementTimeout is the budget the bootstrap runs its catalog
	// reads under, so a diff cannot succeed under a budget the convergence
	// would then fail on, or the reverse. It carries the PostgreSQL
	// statement-budget convention: zero disables the budget explicitly,
	// negative leaves the platform's ambient value alone.
	postgresStatementTimeout time.Duration
	logger                   *slog.Logger
}

// newStorageSchemaService builds the adapter for this server's own storage. It
// is registered on the gRPC endpoint so a control plane can reach the storage
// of a data plane an operator's workstation cannot dial directly.
func (s *Server) newStorageSchemaService() (*storageSchemaAdapter, error) {
	bootTarget, err := storageTargetFor(s.dialect, s.storageDSN)
	if err != nil {
		return nil, fmt.Errorf("read the storage target this instance booted against: %w", err)
	}
	return &storageSchemaAdapter{
		resolveDSN:               s.cfg.StorageDSN,
		bootTarget:               bootTarget,
		dialect:                  s.dialect,
		version:                  attributableVersion(s.version),
		configAllowsDestructive:  s.cfg.Storage.AllowDestructiveSchemaChanges,
		localHosted:              s.localHosted,
		postgresStatementTimeout: s.cfg.Postgres.StatementTimeoutOrDefault(),
		logger:                   s.logger,
	}, nil
}

// registerStorageSchema binds this server's own storage to both surfaces that
// answer for it: the HTTP routes an operator's commands reach, and — through
// the field RegisterGRPC reads — the gRPC endpoint a control plane reaches.
// One adapter serves both, so the answer an operator gets by asking this
// server directly and the answer a control plane gets over gRPC come from one
// implementation rather than two that can drift. A surface left unregistered
// refuses every request as unsupported, which reads as a release too old to
// serve them rather than as wiring that was never done.
func (s *Server) registerStorageSchema(svc *api.Service) error {
	storageSchema, err := s.newStorageSchemaService()
	if err != nil {
		return fmt.Errorf("build storage schema service: %w", err)
	}
	s.storageSchema = storageSchema
	svc.SetStorageSchemaService(storageSchema)
	return nil
}

// parseSupplied holds caller-supplied schema content to the dialect's real
// parser before a diff reads a database with it.
//
// The diff would reach the parser anyway, several layers down, and fail there.
// The difference is who the failure is addressed to: a parse error surfacing
// out of the differ is indistinguishable from the storage database being
// unreachable, so it is reported to the caller as this instance's own fault and
// logged as one. An operator diffing a release whose file they mistyped then
// reads that the server is broken, and goes looking at the server.
//
// Parsing at the door names the file instead, and classifies it as what it is:
// a request this instance understood well enough to refuse.
func (a *storageSchemaAdapter) parseSupplied(desired *api.StorageSchemaSource) error {
	parser, err := ddl.ParserForDialect(a.dialect)
	if err != nil {
		// The dialect is this instance's, fixed at construction — not the
		// caller's — so a dialect with no parser is this instance's problem.
		return fmt.Errorf("parse the supplied storage schema for dialect %s: %w", a.dialect, err)
	}
	for _, name := range slices.Sorted(maps.Keys(desired.Files)) {
		if _, err := parser.Split(desired.Files[name]); err != nil {
			return fmt.Errorf("%w: schema file %q from %s is not valid %s SQL: %w",
				tern.ErrInvalidStorageSchemaRequest, name, desired.Description, a.dialect, err)
		}
	}
	return nil
}

func (a *storageSchemaAdapter) StorageSchemaPlan(ctx context.Context, req *ternv1.StorageSchemaPlanRequest) (*ternv1.StorageSchemaPlanResponse, error) {
	dsn, opts, err := a.target(req.GetAllowDestructive())
	if err != nil {
		return nil, err
	}
	desired, err := a.desiredSchema(req)
	if err != nil {
		return nil, err
	}
	// No budget of its own: PlanStorageSchema imposes StorageSchemaPlanTimeout
	// on whatever context it is handed, so every caller of it — this adapter,
	// the HTTP handler, a boot — gets the same one.
	report, err := api.PlanStorageSchema(ctx, dsn, desired, a.logger, opts...)
	if err != nil {
		return nil, fmt.Errorf("diff storage schema (dialect %s) against %s: %w", a.dialect, desired.Description, err)
	}
	report.AttributeTo(a.version)
	return &ternv1.StorageSchemaPlanResponse{Report: api.StorageSchemaReportProto(report)}, nil
}

// desiredSchema resolves the schema the diff compares the live database
// against: the files the caller sent, or this binary's own when it sent none.
//
// A caller-supplied schema is accepted here and nowhere else. A diff executes
// nothing, so answering "what would this database need in order to match that
// release" is a read however the release's files arrived; the convergence RPC
// has no such field to send, so the schema a convergence runs is always this
// binary's (AV-9).
func (a *storageSchemaAdapter) desiredSchema(req *ternv1.StorageSchemaPlanRequest) (*api.StorageSchemaSource, error) {
	files := req.GetSchemaFiles()
	if len(files) == 0 {
		return api.EmbeddedStorageSchema(a.version), nil
	}
	desired, err := api.StorageSchemaFromFiles(req.GetSchemaSource(), files)
	if err != nil {
		// The caller wrote these files, so the reason goes back to the caller
		// rather than into a log it cannot read (see ErrInvalidStorageSchemaRequest).
		return nil, fmt.Errorf("%w: read the supplied storage schema: %w", tern.ErrInvalidStorageSchemaRequest, err)
	}
	if err := a.parseSupplied(desired); err != nil {
		return nil, err
	}
	a.logger.Info("diffing storage schema against a supplied schema",
		"dialect", a.dialect,
		"schema_source", desired.Description,
		"schema_file_count", len(files),
	)
	return desired, nil
}

func (a *storageSchemaAdapter) StorageSchemaApply(ctx context.Context, req *ternv1.StorageSchemaApplyRequest) (*ternv1.StorageSchemaApplyResponse, error) {
	dsn, opts, err := a.target(req.GetAllowDestructive())
	if err != nil {
		return nil, err
	}
	// The budget comes from the request, not from ctx, and the distinction is
	// the safe one: the convergence is the startup bootstrap, which bounds
	// itself and takes no context, so once the DDL starts a caller hanging up
	// does not stop it. A convergence is a sequence of statements against
	// SchemaBot's own storage, and abandoning it part-way would leave the schema
	// between two releases with nobody watching; running it out leaves a state
	// the next plan can describe. Only the plans either side of it observe ctx,
	// so a caller that disconnects stops waiting for an answer rather than
	// stopping the work.
	//
	// An out-of-range budget is the control plane's to reject before it gets
	// here, but this is a server boundary and the field arrives over the wire,
	// so it is re-checked rather than trusted. The control plane always names
	// the budget it resolved, so a zero here is a caller that could not name
	// one, and it runs under the boot budget: a data plane reached directly
	// must not convert a caller's zero into an hour-long lock hold on behalf
	// of a caller that may have stopped waiting long before.
	budget, err := apitypes.ResolveStorageApplyTimeout(req.GetTimeoutSeconds(), api.EnsureSchemaTimeout)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", tern.ErrInvalidStorageSchemaRequest, err)
	}
	opts = append(opts, api.WithConvergenceTimeout(budget))
	planned, remaining, err := api.ApplyStorageSchema(ctx, dsn, a.logger, opts...)
	if err != nil {
		return nil, fmt.Errorf("converge storage schema (dialect %s): %w", a.dialect, err)
	}
	planned.AttributeTo(a.version)
	remaining.AttributeTo(a.version)
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

// target resolves the storage DSN and the bootstrap options for one call, and
// refuses a DSN that has come to name a database other than the one this
// instance booted against.
func (a *storageSchemaAdapter) target(requestAllowsDestructive bool) (string, []api.EnsureSchemaOption, error) {
	dsn, err := a.resolveDSN()
	if err != nil {
		return "", nil, fmt.Errorf("resolve storage DSN for dialect %s: %w", a.dialect, err)
	}
	if dsn == "" {
		return "", nil, fmt.Errorf("storage DSN not configured for dialect %s; the storage schema surface has no database to read", a.dialect)
	}
	if err := a.checkBootTarget(dsn); err != nil {
		return "", nil, err
	}
	allowDestructive, err := a.destructivePolicy(requestAllowsDestructive)
	if err != nil {
		return "", nil, err
	}
	return dsn, []api.EnsureSchemaOption{
		api.WithDialect(a.dialect),
		api.WithAllowDestructiveSchemaChanges(allowDestructive),
		api.WithPostgresStatementTimeout(a.postgresStatementTimeout),
	}, nil
}

// checkBootTarget refuses a DSN that no longer names the database this
// instance booted against.
//
// The DSN is re-resolved per call so a rotated credential is picked up without
// a restart, and a rotation is the only movement that may be honored. A DSN
// that now names a different server or a different database describes some
// other SchemaBot's storage: reading it would attribute another instance's
// drift to this one, and converging it would run this binary's embedded schema
// against a database it never booted on. Both are refused, which is the
// binding the adapter documents and the one AV-9 rests on.
func (a *storageSchemaAdapter) checkBootTarget(dsn string) error {
	resolved, err := storageTargetFor(a.dialect, dsn)
	if err != nil {
		return fmt.Errorf("read the storage target the current configuration names: %w", err)
	}
	if resolved == a.bootTarget {
		return nil
	}
	a.logger.Error("refusing a storage schema request: the configured storage has moved since this instance booted",
		"dialect", a.dialect,
		"boot_target", a.bootTarget.String(),
		"configured_target", resolved.String(),
	)
	return fmt.Errorf("the configured storage now names %s, but this instance booted against %s; "+
		"the storage schema surface only answers for the database this instance is running on, so restart it to adopt the new storage", resolved, a.bootTarget)
}

// destructivePolicy resolves whether this call may run destructive statements:
// the deployment's policy widened by an explicit per-request opt-in, never
// narrowed by its absence — and never widened at all on a locally hosted
// server, which refuses the request instead.
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
//
// Local hosting is the exception because there the consent cannot be trusted to
// mean what it says: the local runtime has no authorization to identify who
// issued the command, and the storage it would destroy is a real deployment's
// whenever a local host has been pointed at one. It refuses the whole request
// rather than running the safe remainder under a flag it ignored, so an
// operator learns their opt-in did not apply instead of reading a report that
// looks like it did (AZ-6).
func (a *storageSchemaAdapter) destructivePolicy(requestAllowsDestructive bool) (bool, error) {
	if requestAllowsDestructive && a.localHosted {
		a.logger.Warn("refusing a storage schema request that opts in to destructive statements: this server is locally hosted",
			"dialect", a.dialect,
			"boot_target", a.bootTarget.String(),
		)
		return false, fmt.Errorf("%w: this server is locally hosted, which never runs destructive storage schema statements; "+
			"re-run without the destructive opt-in to converge everything else, or address a deployed server to run them", tern.ErrInvalidStorageSchemaRequest)
	}
	return a.configAllowsDestructive || requestAllowsDestructive, nil
}
