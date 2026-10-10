package serve

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
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
// Two things a caller may influence, and neither moves the database. Whether
// destructive statements run only ever widens what the local config already
// allows, and on a locally hosted server not even that (see
// checkDestructiveOptIn). A desired schema replaces the files both RPCs read,
// on a diff and on a convergence alike (see desiredSchema), because an operator
// rolling a later release has to be able to converge this storage to it before
// its first pod starts. What a supplied schema cannot do is widen what the
// bootstrap permits: the destructive refusal and the manual-remediation gate
// decide on supplied files the same way they decide on embedded ones (AV-9).
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
	// an operator convergence must too — an operator may name which schema
	// runs, never the policy it runs under, and otherwise the two would
	// disagree on exactly the deployments that opted in.
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
	desired, err := a.desiredSchema(req.GetSchemaFiles(), req.GetSchemaSource(), "diff")
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

// desiredSchema resolves the schema a request works from: the files the caller
// sent, or this binary's own when it sent none.
//
// Both RPCs resolve it here, so the schema a caller previews is the schema they
// converge. Sending files is how an operator asks about — and readies — a
// release this binary does not carry, which is the question a deploy actually
// has. What the supplied files never do is decide what may run: the convergence
// applies the bootstrap's own destructive refusal and manual-remediation gate
// to them, unchanged (AV-9).
//
// operation names what the files are for, so the log line says whether a
// supplied schema was read for a diff or run against the database.
//
// A name with no files to go with it is refused here rather than at a
// transport, because the tern gRPC server reaches this adapter without passing
// the HTTP API's validation, and this is the one half of that pair the
// defaulting below would swallow: it would resolve to the embedded schema and
// converge it, running one release's schema for a caller that named another's.
// Files with no name are refused by StorageSchemaFromFiles, which cannot
// attribute a report without one.
func (a *storageSchemaAdapter) desiredSchema(files map[string]string, schemaSource, operation string) (*api.StorageSchemaSource, error) {
	if len(files) == 0 {
		if strings.TrimSpace(schemaSource) != "" {
			return nil, fmt.Errorf("%w: schema_source %q was sent without schema_files: with no files this server's own embedded schema would run and be reported under that name; send the files, or drop schema_source to use the embedded schema",
				tern.ErrInvalidStorageSchemaRequest, schemaSource)
		}
		return api.EmbeddedStorageSchema(a.version), nil
	}
	desired, err := api.StorageSchemaFromFiles(schemaSource, files)
	if err != nil {
		// The caller wrote these files, so the reason goes back to the caller
		// rather than into a log it cannot read (see ErrInvalidStorageSchemaRequest).
		return nil, fmt.Errorf("%w: read the supplied storage schema: %w", tern.ErrInvalidStorageSchemaRequest, err)
	}
	if err := a.parseSupplied(desired); err != nil {
		return nil, err
	}
	a.logger.Info("storage schema request carries a supplied schema",
		"operation", operation,
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
	budget, err := convergenceBudget(req)
	if err != nil {
		return nil, err
	}
	opts = append(opts, api.WithConvergenceTimeout(budget))
	desired, err := a.desiredSchema(req.GetSchemaFiles(), req.GetSchemaSource(), "converge")
	if err != nil {
		return nil, err
	}
	// ApplyStorageSchema cancels its convergence when its context is cancelled
	// — that is what lets an operator at a terminal stop a run they are
	// watching. This context is a request's, and cancelling it means the
	// connection dropped, not that anyone decided anything. Stripping the
	// cancellation is what keeps a lost TCP connection from taking a table
	// copy with it; the budget above is still what bounds the run.
	planned, remaining, err := api.ApplyStorageSchema(context.WithoutCancel(ctx), dsn, desired, a.logger, opts...)
	if err != nil {
		return nil, fmt.Errorf("converge storage schema (dialect %s) to %s: %w", a.dialect, desired.Describe(), err)
	}
	planned.AttributeTo(a.version)
	remaining.AttributeTo(a.version)
	a.logger.InfoContext(ctx, "storage schema convergence answered",
		"dialect", a.dialect,
		"database", remaining.Database,
		"schema_source", remaining.SchemaSource,
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

// convergenceBudget resolves the budget one convergence runs under from the
// request that asked for it.
//
// The budget comes from the request, not from ctx, and the distinction is the
// safe one. A convergence is a sequence of statements against SchemaBot's own
// storage, and abandoning it part-way would leave the schema between two
// releases with nobody watching; running it out leaves a state the next plan
// can describe.
//
// ApplyStorageSchema does observe a context, which is what lets an operator at
// a terminal stop a run they are watching. That authority is not this caller's
// to pass on: a request context is cancelled by a dropped connection, which is
// not a decision anyone made about the storage every instance depends on, so
// the call site strips the cancellation before handing ctx over. For a
// convergence reached over the wire the budget is what ends it, and a caller
// that disconnects stops waiting for an answer rather than stopping the work.
//
// An out-of-range budget is the control plane's to reject before it gets
// here, but this is a server boundary and the field arrives over the wire, so
// it is re-checked rather than trusted. The control plane always names the
// budget it resolved, so a zero here is a caller that could not name one, and
// it runs under the boot budget: a data plane reached directly must not
// convert a caller's zero into an hour-long lock hold on behalf of a caller
// that may have stopped waiting long before (AV-11).
func convergenceBudget(req *ternv1.StorageSchemaApplyRequest) (time.Duration, error) {
	budget, err := apitypes.ResolveStorageApplyTimeout(req.GetTimeoutSeconds(), api.EnsureSchemaTimeout)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", tern.ErrInvalidStorageSchemaRequest, err)
	}
	return budget, nil
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
	if err := a.checkDestructiveOptIn(requestAllowsDestructive); err != nil {
		return "", nil, err
	}
	return dsn, []api.EnsureSchemaOption{
		api.WithDialect(a.dialect),
		api.WithDestructiveSchemaChangePolicy(api.ConfiguredDestructivePolicy(a.configAllowsDestructive), requestAllowsDestructive),
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
// drift to this one, and converging it would run DDL against a database this
// instance never booted on. Both are refused, which is the binding the adapter
// documents and the one AV-9 rests on.
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

// checkDestructiveOptIn admits or refuses a per-request opt-in to destructive
// statements. It resolves nothing: the two policies travel separately into the
// bootstrap options, which is what lets a report say what this call runs and
// what a boot runs without the second being inferred from the first.
//
// What this decides is whether the opt-in may be honored at all — it is never
// honored on a locally hosted server, which refuses the request rather than
// widening anything.
//
// The two directions are not symmetric and the asymmetry is the point. A
// deployment that configured allow_destructive_schema_changes has already made
// the decision for every boot; a convergence that ignored it would run less
// than the next boot runs — an apply refusing on a deployment where a boot
// proceeds, which is the deployment an operator converging ahead of a roll most
// needs it not to. In the other direction, a request opting in is exactly the
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
func (a *storageSchemaAdapter) checkDestructiveOptIn(requestAllowsDestructive bool) error {
	if requestAllowsDestructive && a.localHosted {
		a.logger.Warn("refusing a storage schema request that opts in to destructive statements: this server is locally hosted",
			"dialect", a.dialect,
			"boot_target", a.bootTarget.String(),
		)
		return fmt.Errorf("%w: this server is locally hosted, which never runs destructive storage schema statements; "+
			"re-run without the destructive opt-in to converge everything else, or address a deployed server to run them", tern.ErrInvalidStorageSchemaRequest)
	}
	return nil
}
