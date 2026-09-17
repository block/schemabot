package api

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/engine/spirit"
	"github.com/block/schemabot/pkg/postgresconn"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/spirit/pkg/utils"
)

// Storage-schema inspection answers one question an operator has during a
// deploy that did not converge: which storage DDL is still outstanding, right
// now, on this database.
//
// It answers from the live database and from schema files — never from a
// version pin. A consumer's go.mod pin says which release a host binary *was
// built against*; it says nothing about what the storage it talks to has
// actually converged to, and the two diverge exactly when a deploy has failed
// to converge. Since that is precisely when someone computes this diff, the pin
// is the one input that cannot be trusted, so no input to this package is a
// version: the desired side is always files, and the report names which ones
// (see StorageSchemaSource).
//
// The diff is the bootstrap's own diff, not a second implementation of it. On
// MySQL that is Spirit's differ; on PostgreSQL it is postgresSchemaDriftFor.
// A statement this reports against this binary's own embedded schema is a
// statement a boot of this binary would plan, because it came from the same
// call.

// StorageSchemaPlanTimeout bounds a read-only storage-schema diff. It is far
// below EnsureSchemaTimeout because a diff does no DDL: it reads the live
// catalog and compares. Bounding it separately keeps an unreachable storage
// database from holding an operator's request — or a control-plane request
// thread — for the whole bootstrap budget.
const StorageSchemaPlanTimeout = 30 * time.Second

// PlanStorageSchema reports the storage DDL outstanding between a desired
// schema and the live storage database at dsn. It is strictly read-only: it
// opens connections, reads the catalog, computes a diff, and reads whether the
// bootstrap lock is held. It executes no DDL, takes no lock, and writes
// nothing, so it is safe to run at any time, including against a database an
// apply is converging right now — which is the case it exists to describe.
//
// desired is the schema to compare against; nil is the embedded schema of this
// binary, which is what a boot would converge to. A caller deploying a later
// release supplies that release's files instead (see StorageSchemaSource), and
// the report attributes the answer to whichever was used.
//
// The dialect selects the differ, mirroring EnsureSchema's dispatch, and fails
// closed for a dialect without one rather than running another family's
// catalog queries. Pass the same options EnsureSchema is wired with so the
// report describes what a boot would decide;
// WithAllowDestructiveSchemaChanges only labels the report here, since a diff
// executes nothing either way.
func PlanStorageSchema(ctx context.Context, dsn string, desired *StorageSchemaSource, logger *slog.Logger, opts ...EnsureSchemaOption) (*StorageSchemaReport, error) {
	// The budget is imposed here rather than left to the caller: a control
	// plane thread and an operator's terminal both reach this through contexts
	// that may carry no deadline at all, and an unreachable storage database
	// would hold either of them open indefinitely.
	ctx, cancel := context.WithTimeout(ctx, StorageSchemaPlanTimeout)
	defer cancel()

	o := newEnsureSchemaOptions(opts...)
	var report *StorageSchemaReport
	var err error
	switch o.dialect {
	case schema.DialectMySQL:
		report, err = planMySQLStorageSchema(ctx, dsn, desired, o)
	case schema.DialectPostgres:
		report, err = planPostgresStorageSchema(ctx, dsn, desired, o)
	default:
		return nil, fmt.Errorf("no storage schema differ for storage dialect %q (supported: %q, %q)", o.dialect, schema.DialectMySQL, schema.DialectPostgres)
	}
	if err != nil {
		return nil, err
	}
	// Read after the diff, not before. The diff is the slow half, and a lock
	// taken while it ran is one this report should carry: an operator about to
	// act on these statements needs to know somebody else is already running
	// them, and a stale false is the reading that costs them a silent hour
	// waiting on a lock.
	report.ConvergenceInFlight = storageConvergenceInFlight(ctx, dsn, o.dialect, logger)

	// The differs themselves stay quiet — the MySQL one is handed a discarding
	// handler so Spirit's planning chatter does not read as a schema change
	// engine running — so this is the one place a diff leaves a server-side
	// trace. Without it a drifting storage database is visible only to whoever
	// ran the command.
	logger.Info("planned storage schema",
		"dialect", report.Dialect,
		"database", report.Database,
		"schema_source", report.SchemaSource,
		"outstanding_count", len(report.Outstanding),
		"destructive_count", len(report.Destructive),
		"manual_count", len(report.Manual),
		"destructive_allowed", report.DestructiveAllowed,
		"convergence_in_flight", report.ConvergenceInFlight,
	)
	return report, nil
}

// storageApplyOptions puts the operator's convergence budget in front of a
// caller's options, so every entry point onto the deliberate path gets it
// without having to remember to ask. Caller options are applied after and so
// still win, which is what lets a command honor an operator's --timeout.
func storageApplyOptions(opts []EnsureSchemaOption) []EnsureSchemaOption {
	return append([]EnsureSchemaOption{WithConvergenceTimeout(apitypes.DefaultStorageApplyTimeout)}, opts...)
}

// ApplyStorageSchema converges the storage database at dsn and reports what it
// found and what it left behind.
//
// The convergence is the startup bootstrap, called unchanged: the same differ,
// the same destructive-change refusal, the same advisory lock serializing it
// against every other instance and against another operator running this at
// the same time. Nothing about the policy is re-decided here — an operator
// command that converged storage differently from a boot would be a second
// implementation of the one path that must not have two.
//
// It takes no schema source, and the absence is the safety property rather than
// an omission: a convergence runs the schema embedded in the binary running it,
// so "apply is what a boot does" holds by construction (AV-9). A caller that
// wants a later release's schema on a database runs that release's binary.
//
// Two reports bracket the run, because "what happened" and "what is left" are
// different questions and an operator mid-incident needs both:
//
//	planned    what was outstanding before, including refusals
//	remaining  what is still outstanding after
//
// remaining is empty on a clean convergence. It carries the refused
// destructive statements when the database holds state this binary's schema
// does not declare, which is the expected steady state during a rollback
// (AV-9) rather than a failure.
//
// ctx bounds the two diffs, not the convergence between them: EnsureSchema
// builds its own context from the convergence budget so that a cancelled
// request cannot abandon a table copy half-done. A caller whose own deadline is
// shorter than that budget will therefore return before the convergence does,
// and must not read its own timeout as the apply having stopped.
//
// That budget defaults to DefaultStorageApplyTimeout here rather than to the
// boot budget, and the default belongs on this function rather than on each of
// its callers. This is the deliberate path by definition — a convergence
// reached through it was asked for by somebody, whether over the RPC or from a
// terminal holding the DSN — so every entry point should get the operator's
// ceiling without having to remember to ask for it. A caller passing
// WithConvergenceTimeout still wins, since caller options are applied last.
func ApplyStorageSchema(ctx context.Context, dsn string, logger *slog.Logger, opts ...EnsureSchemaOption) (planned, remaining *StorageSchemaReport, err error) {
	opts = storageApplyOptions(opts)
	planned, err = PlanStorageSchema(ctx, dsn, nil, logger, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("diff storage schema before converging it: %w", err)
	}
	if len(planned.Manual) > 0 {
		// The convergence would refuse the whole drift set anyway. Returning
		// here reports every problem at once with its remediation, instead of
		// surfacing the bootstrap's joined error string as an opaque failure.
		logger.Warn("refusing to converge storage schema because changes need manual remediation",
			"dialect", planned.Dialect,
			"database", planned.Database,
			"manual_count", len(planned.Manual),
			"outstanding_count", len(planned.Outstanding),
		)
		return planned, planned, nil
	}
	// A converged catalog is not a reason to skip the bootstrap. EnsureSchema
	// also removes the schema change engine's leftover tables, which outlive an
	// interrupted convergence and are invisible to a catalog diff — returning
	// here would report the storage clean while leaving them on it. The
	// bootstrap is cheap in that case by construction: it takes no lock and
	// does no DDL when there is nothing to do.
	// The budget is logged because it is the one thing about this run an
	// operator cannot see from the reports either side of it, and it is the
	// first question asked when a convergence is still going or a booting pod
	// reports losing the lock wait.
	logger.Info("converging storage schema on operator request",
		"dialect", planned.Dialect,
		"database", planned.Database,
		"outstanding_count", len(planned.Outstanding),
		"destructive_count", len(planned.Destructive),
		"destructive_allowed", planned.DestructiveAllowed,
		"convergence_timeout", newEnsureSchemaOptions(opts...).convergenceTimeout,
	)
	if err := EnsureSchema(dsn, logger, opts...); err != nil {
		return planned, nil, fmt.Errorf("converge storage schema on database %q (%s): %w", planned.Database, planned.Dialect, err)
	}

	remaining, err = PlanStorageSchema(ctx, dsn, nil, logger, opts...)
	if err != nil {
		// The convergence succeeded; only the confirming read failed. Report
		// that distinctly — an operator must not read a failed verification as
		// a failed apply and run it again looking for a different answer.
		return planned, nil, fmt.Errorf("storage schema converged on database %q (%s) but re-reading it to confirm failed: %w", planned.Database, planned.Dialect, err)
	}
	logger.Info("storage schema convergence complete",
		"dialect", remaining.Dialect,
		"database", remaining.Database,
		"applied_count", planned.appliedCount(),
		"remaining_count", remaining.remainingCount(),
	)
	return planned, remaining, nil
}

// planMySQLStorageSchema diffs the desired MySQL schema files against the live
// storage database with Spirit's differ — the same Plan call ensureMySQLSchema
// makes, so the two cannot disagree about what a boot would run. Spirit emits
// one combined ALTER per table, and partitionDestructiveChanges sorts those
// statements the way the bootstrap would, so each one is reported as the boot
// would treat it rather than as a statement whose disposition an operator has to
// guess. A statement the boot would reduce to its additions is reported that
// way too: the additions under Outstanding, the withheld clauses under
// Destructive, which is what the two halves will actually do.
func planMySQLStorageSchema(ctx context.Context, dsn string, desired *StorageSchemaSource, o ensureSchemaOptions) (*StorageSchemaReport, error) {
	report := &StorageSchemaReport{
		Dialect:            schema.DialectMySQL,
		SchemaSource:       desired.Describe(),
		DestructiveAllowed: o.allowDestructive,
	}

	// The database identity is what makes the report readable as being about
	// one database on one server. Unlike the bootstrap's preamble, a failure
	// here is fatal: the bootstrap can converge without knowing the name, but a
	// report that cannot say which database it read is one an operator cannot
	// act on.
	diag, err := diagnoseStorageTarget(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("read storage target identity: %w", err)
	}
	report.Database = diag.database
	report.Host = diag.hostname

	schemaFiles, err := desired.mysqlSchemaFiles()
	if err != nil {
		return nil, err
	}

	eng := spirit.New(spirit.Config{Logger: slog.New(slog.DiscardHandler)})
	planResult, err := eng.Plan(ctx, &engine.PlanRequest{
		Database:    storageSchemaNamespace,
		SchemaFiles: schemaFiles,
		Credentials: &engine.Credentials{DSN: dsn},
	})
	if err != nil {
		return nil, fmt.Errorf("plan storage schema against database %q: %w", diag.database, err)
	}
	if planResult.NoChanges {
		return report, nil
	}

	allowed, refused := partitionDestructiveChanges(planResult.Changes)
	for _, tc := range flatTableChanges(allowed) {
		operation, err := storageSchemaOperation(tc.Operation)
		if err != nil {
			return nil, fmt.Errorf("classify storage schema statement on table %q of database %q: %w", tc.Table, diag.database, err)
		}
		report.Outstanding = append(report.Outstanding, StorageSchemaStatement{
			Table:     tc.Table,
			Operation: operation,
			DDL:       tc.DDL,
		})
	}
	for _, r := range refused {
		operation, err := storageSchemaOperation(r.change.Operation)
		if err != nil {
			return nil, fmt.Errorf("classify refused storage schema statement on table %q of database %q: %w", r.change.Table, diag.database, err)
		}
		report.Destructive = append(report.Destructive, StorageSchemaStatement{
			Table:     r.change.Table,
			Operation: operation,
			DDL:       r.change.DDL,
			Reason:    r.reason,
		})
	}
	return report, nil
}

// storageSchemaOperation names a MySQL statement type in the report's
// vocabulary. A type the storage schema cannot contain is an error rather than
// an "unknown" label: the storage schema is a fixed set of tables, so a
// RENAME or a view arriving here means the differ saw something this package
// does not understand, and labelling it would hide that in a report an
// operator is about to act on.
func storageSchemaOperation(t ddl.StatementType) (string, error) {
	switch t {
	case ddl.StatementCreateTable:
		return storageSchemaOpCreateTable, nil
	case ddl.StatementAlterTable:
		return storageSchemaOpAlterTable, nil
	case ddl.StatementDropTable:
		return storageSchemaOpDropTable, nil
	default:
		return "", fmt.Errorf("unexpected statement type %q in a storage schema diff; the storage schema is converged with CREATE TABLE, ALTER TABLE and DROP TABLE only", t)
	}
}

// planPostgresStorageSchema diffs the desired PostgreSQL schema files against
// the live storage database with the additive convergence's own drift scan, so
// the report is exactly what ensurePostgresSchema would decide. The convergence
// never drops or alters an existing object, so the report has no destructive
// set; what it does have is the manual-remediation set, whose entries abort a
// whole convergence pass rather than being skipped.
func planPostgresStorageSchema(ctx context.Context, dsn string, desired *StorageSchemaSource, o ensureSchemaOptions) (*StorageSchemaReport, error) {
	report := &StorageSchemaReport{Dialect: schema.DialectPostgres, SchemaSource: desired.Describe()}

	tables, files, err := desired.postgresSchemaFiles()
	if err != nil {
		return nil, err
	}

	db, err := postgresconn.Open(dsn, postgresconn.WithStatementTimeout(o.postgresStatementTimeout))
	if err != nil {
		return nil, fmt.Errorf("open storage database: %w", err)
	}
	defer utils.CloseAndLog(db)
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping storage database: %w", err)
	}
	// inet_server_addr() is null over a Unix socket and on some managed
	// platforms, so the host is coalesced to empty rather than failing the
	// read: a report that names the database but not the server is still
	// actionable, and one that failed outright is not.
	if err := db.QueryRowContext(ctx,
		"SELECT current_database(), COALESCE(host(inet_server_addr()), '')",
	).Scan(&report.Database, &report.Host); err != nil {
		return nil, fmt.Errorf("read storage target identity: %w", err)
	}

	drift, err := postgresSchemaDriftFor(ctx, db, tables, files)
	if err != nil {
		return nil, fmt.Errorf("inspect storage schema on database %q: %w", report.Database, err)
	}
	// Walk tables rather than the drift map so the statements come out in the
	// order the convergence would run them, which is the order they are safe
	// to paste and run by hand.
	for _, table := range tables {
		for _, change := range drift[table] {
			statement := StorageSchemaStatement{
				Table:     table,
				Operation: change.operation,
				DDL:       change.ddl,
			}
			if change.manualReason == "" {
				report.Outstanding = append(report.Outstanding, statement)
				continue
			}
			statement.Reason = postgresManualProblem(table, change)
			report.Manual = append(report.Manual, statement)
		}
	}
	return report, nil
}

// newEnsureSchemaOptions applies opts over the defaults every entry point
// shares, so PlanStorageSchema and EnsureSchema start from the same policy for
// an option a caller did not set.
func newEnsureSchemaOptions(opts ...EnsureSchemaOption) ensureSchemaOptions {
	o := ensureSchemaOptions{
		dialect:                  schema.DialectMySQL,
		convergenceTimeout:       EnsureSchemaTimeout,
		postgresStatementTimeout: DefaultPostgresStatementTimeout,
	}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}
