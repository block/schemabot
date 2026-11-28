package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/block/pg-sprite/pkg/dbconn"
	"github.com/block/pg-sprite/pkg/executor"
	"github.com/block/pg-sprite/pkg/migrate"
	"github.com/block/pg-sprite/pkg/planner"
	"github.com/block/pg-sprite/pkg/preflight"
	"github.com/block/pg-sprite/pkg/progress"
	"github.com/block/pg-sprite/pkg/schemadiff"
	pgstatement "github.com/block/pg-sprite/pkg/statement"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/ui"
)

const (
	optimisticLockTimeout    = 3 * time.Second
	optimisticStatementLimit = 30 * time.Second

	// optimisticApplyCeiling bounds one whole background apply client-side:
	// pool dial, preflight, and every execution attempt with its backoff.
	// Server-side statement/lock timeouts bound queries once a session is
	// healthy, but they cannot unwedge a hung dial or a black-holed
	// connection — the ceiling guarantees the drive always reaches a
	// terminal progress state. Generously above the worst legitimate run of
	// the retry path (retry attempts x statement limit plus backoffs) so it
	// only fires on genuine hangs. A concurrent index build does not run
	// under this constant: its ceiling is the configured build bound plus
	// concurrentIndexHeadroom. The constructor seeds the engine's field of
	// the same name from it; the apply reads the field.
	optimisticApplyCeiling = 5 * time.Minute

	// executorProgressReadTimeout bounds one read of the executor's tracker.
	// For an active concurrent index build that read is a single-row query
	// on the session the executor reserved for the build's failure verdict,
	// and the tracker serializes it against the executor's own end-of-build
	// fence — so a poll that hangs on a half-open socket would hold up the
	// apply's verdict and its terminal publish. A deadline of the engine's
	// own keeps that coupling bounded regardless of what the caller's
	// context does.
	//
	// The bound sits above the statement_timeout the apply pool runs its
	// sessions under (spritePoolConfig leaves it at pg-sprite's default) so
	// that on a live connection the server ends a slow read first: a
	// statement_timeout cancels the query and hands the session back
	// intact, whereas a client deadline expiring mid-query closes the
	// connection — and that connection is the one the build's failure
	// verdict needs. The engine's deadline is therefore the bound of last
	// resort, for the socket the server's cancellation can no longer reach.
	executorProgressReadTimeout = dbconn.DefaultStatementTimeout + executorProgressReadHeadroom

	// executorProgressReadHeadroom is how far the read deadline sits past
	// the session's statement_timeout: long enough that the server's
	// cancellation error reaches the client before the client gives up on
	// the socket.
	executorProgressReadHeadroom = 5 * time.Second

	// concurrentIndexHeadroom is how far a concurrent index apply's ceiling
	// sits above the configured build bound. The ceiling wraps the whole
	// apply while the build bound — the build's server-side statement
	// timeout, or the recovery's deadline — starts only when the build
	// starts, so the gap is the time granted to everything executeOptimistic
	// runs before the build: the pool dial, the privilege check, the
	// preflight table read and the partition admission facts lookup. As long
	// as that setup finishes within the headroom, the bound is what ends an
	// over-long build — the server's timer fires first and exhaustion
	// surfaces as the typed budget verdict — and the build gets the full
	// bound the operator configured; setup that outruns the headroom shortens
	// the build by the excess, because the ceiling then fires first and the
	// outcome degrades to a cancellation. Nothing after the build
	// charges the gap: pg-sprite runs its post-failure catalog verdict on
	// a detached context of its own, the cleanup of an operator-cancelled
	// build runs detached under cancelledIndexCleanupBound, and the terminal
	// publish reads the tracker the same way. The bound is sized to cover
	// setup even when every one of its steps runs to the server-side limit
	// the pool sets for it — the connect timeout for the dial, the
	// statement_timeout for each preflight read.
	concurrentIndexHeadroom = 2 * time.Minute

	// cancelledIndexCleanupBound is the whole budget for removing the
	// invalid index an operator-cancelled build left behind. The cleanup
	// runs after the apply context is cancelled and before the terminal
	// outcome is published, so Cancel's settle wait spans it: the bound
	// must stay well inside cancelSettleTimeout, or a slow cleanup would
	// turn a cancel that is about to succeed into a not-settled answer.
	// The work it covers is a session dial, catalog reads, and the
	// abandonment proof and drop, each of which pg-sprite runs under its
	// own short lock timeout, so the bound is seconds; when it is spent the
	// entry stays for the invalid-index recovery guidance and the apply
	// still settles cancelled.
	cancelledIndexCleanupBound = 30 * time.Second
)

type nativeApply struct {
	rowSecurity      *pgstatement.DesiredWithRowSecurity
	reviewedSecurity []string
	// createsTable marks a row security apply that creates its table, in
	// the same transaction as the row security.
	createsTable     bool
	namespace        string
	table            string
	sql              string
	steps            int
	concurrentIndex  bool
	partitionedIndex bool
	enumValueAdd     bool
	// dropped is the plan's record of the live object the statement drops,
	// which the apply checks before dropping it: under the table's lock, or
	// for an index drop, right before the concurrent drop.
	dropped *schemadiff.DroppedObject
	// indexDrop is set for a DROP INDEX CONCURRENTLY of the index dropped
	// names, which runs through pg-sprite's concurrent index drop.
	indexDrop bool
	// adopts is the plan's record of the index the statement's constraint
	// takes over, which the apply checks under the table's lock before the
	// constraint attaches it.
	adopts *schemadiff.IndexAdoption
	// replacement is the plan's record of the index that replaces the one
	// the statement drops, which the apply checks right before the drop.
	replacement *schemadiff.IndexReplacement
	// validates is the plan's record of the live NOT VALID constraint the
	// statement validates, which the apply checks under the validation's
	// lock before validating it.
	validates *schemadiff.ConstraintValidation
	// database names the target for the logs and metrics of a drop.
	database string
	// droppedTable is the definition the plan recorded for a table the
	// statement drops whole, which pg-sprite checks under the table's lock.
	droppedTable               string
	concurrentIndexMaxDuration time.Duration
	// validateMaxDuration bounds the scan of a statement that validates a
	// constraint; it is set only for such a statement.
	validateMaxDuration time.Duration
	cancelRequested     func() bool
}

// durationBoundedValidation reports whether the change validates a NOT VALID
// constraint. The scan takes SHARE UPDATE EXCLUSIVE and rewrites nothing, so
// reads and writes continue through it however long it runs: it is bounded by
// the validation duration instead of the per-statement limit, the table size
// ceiling, and the optimistic apply ceiling. Its lock wait keeps the short
// lock budget.
func (c nativeApply) durationBoundedValidation() bool {
	return c.validates != nil
}

// durationBoundedIndexBuild reports whether the change is an index build run
// under the concurrent index duration envelope instead of the table size
// ceiling and the optimistic apply ceiling: a concurrent index build, or a
// partitioned index build, whose every partition is one concurrent build. It
// is also what makes an apply cancellable, since only these builds run long
// enough for an operator to need to end them.
func (c nativeApply) durationBoundedIndexBuild() bool {
	return c.concurrentIndex || c.partitionedIndex
}

// buildShape is how an operator cancel reaches the change's work.
type buildShape struct {
	// cancellable is set for an index build, concurrent or partition by
	// partition, the one PostgreSQL change an operator can cancel.
	cancellable bool
	// partitioned is set for a partitioned index build, which runs one
	// concurrent build per partition, so a signal to the build in flight
	// does not reach the partitions after it.
	partitioned bool
	// haltable is set for the work a shutdown or a drive handing the apply
	// back ends instead of waiting out: the changes bounded by a long
	// duration rather than the per-statement limit. An index build leaves an
	// invalid index the next driver recovers, and a constraint validation
	// leaves the constraint NOT VALID for the next driver to validate, so
	// neither leaves work half-applied.
	haltable bool
	// validationBound is set for a constraint validation: how long its scan
	// may run before it is cut off, which a refused cancel names as when the
	// validation ends on its own.
	validationBound time.Duration
}

// buildShape records the change's shape when the apply is admitted, before
// the drive sets the change's own bounds, so the validation bound comes from
// the engine's configuration.
func (c nativeApply) buildShape(validateMaxDuration time.Duration) buildShape {
	var validationBound time.Duration
	if c.durationBoundedValidation() {
		validationBound = validateMaxDuration
	}
	return buildShape{
		cancellable:     c.durationBoundedIndexBuild(),
		partitioned:     c.partitionedIndex,
		haltable:        c.durationBoundedIndexBuild() || c.durationBoundedValidation(),
		validationBound: validationBound,
	}
}

// targetConn carries one background apply's connection inputs: the raw DSN
// (normalized at open time) and the CA bundle path the pool verifies the
// target against — empty when the embedded RDS trust or the DSN's own
// settings apply.
type targetConn struct {
	dsn        string
	caCertPath string
}

// Apply starts one native-safe PostgreSQL change under pg-sprite's bounded
// optimistic executor. A greenfield table and its declared indexes are
// admitted as one create-set task whose sequence steps commit independently.
func (e *Engine) Apply(ctx context.Context, req *engine.ApplyRequest) (*engine.ApplyResult, error) {
	change, err := validateOptimisticApply(req)
	if err != nil {
		return nil, err
	}
	// Resolve the CA reference at acceptance so a trust root the engine
	// cannot honor refuses the apply before any background work is queued.
	caPath, err := caCertPath(req.Credentials)
	if err != nil {
		return nil, fmt.Errorf("apply PostgreSQL database %q: %w", req.Database, err)
	}
	// The bundle itself is held to the same bar: an unreadable or
	// certificate-free bundle can never dial, so it refuses the apply here
	// rather than failing the background drive. This proves the reference is
	// honorable at acceptance; the pool re-reads the path when it dials.
	if caPath != "" {
		if _, err := loadCABundle(caPath); err != nil {
			return nil, fmt.Errorf("apply PostgreSQL database %q: %w", req.Database, err)
		}
	}

	// One tracker per apply: pg-sprite's executor records each step it
	// starts on it, and Progress reads the position back, so the poller
	// sees the step in flight rather than only the accept and terminal
	// states the engine itself publishes.
	tracker, err := progress.NewTracker(progress.WallClock{})
	if err != nil {
		return nil, fmt.Errorf("apply PostgreSQL database %q: %w", req.Database, err)
	}

	logger := req.Logger
	if logger == nil {
		logger = slog.Default()
	}
	started := time.Now()
	key := progressIdentity(req.ResumeState)
	// An accepted apply must survive the request that accepted it, so the
	// drive's context is detached from the caller. It is cancellable so the
	// engine itself can end the drive — a cancel of last resort, and the
	// halt on shutdown.
	applyCtx, cancelApply := context.WithCancel(context.WithoutCancel(ctx))
	done := e.claimProgress(key, progressResult(engine.StateRunning, "preflight", started, change, ""), tracker, logger, change.buildShape(e.validateMaxDuration), cancelApply, engine.WorkOwnerFromContext(ctx))
	conn := targetConn{dsn: req.Credentials.DSN, caCertPath: caPath}
	e.wg.Go(func() {
		defer close(done)
		defer cancelApply()
		e.runOptimisticApply(applyCtx, conn, change, key, started, logger, tracker)
	})

	return &engine.ApplyResult{
		Accepted:    true,
		Message:     "Started native-safe PostgreSQL schema change",
		ResumeState: req.ResumeState,
	}, nil
}

// validateOptimisticApply validates only engine-level applicability. Blocked
// verdicts are enforced at admission by storage.Plan.BlockedApplyError before
// tasks are created; task rows are rebuilt from stored plans and do not carry
// the plan's ExecutionMode verdict.
func validateOptimisticApply(req *engine.ApplyRequest) (nativeApply, error) {
	if req == nil {
		return nativeApply{}, fmt.Errorf("apply PostgreSQL schema: request is required")
	}
	if req.Credentials == nil || req.Credentials.DSN == "" {
		return nativeApply{}, fmt.Errorf("apply PostgreSQL database %q: DSN credentials are required", req.Database)
	}
	if len(req.Changes) != 1 || len(req.Changes[0].TableChanges) != 1 {
		return nativeApply{}, fmt.Errorf("apply PostgreSQL database %q: native-safe increment requires exactly one planned change", req.Database)
	}
	tc := req.Changes[0].TableChanges[0]
	if operation, err := pgstatement.ParseRowSecurityChange(tc.DDL); err == nil {
		return validateRowSecurityApply(req, operation)
	}

	hasRLS, err := pgstatement.HasRowSecurityDeclaration(tc.DDL)
	if err != nil {
		slog.Warn("PostgreSQL apply admission rejected planned DDL", "database", req.Database, "table", tc.Table, "error", err)
		return nativeApply{}, fmt.Errorf("apply PostgreSQL table %q: planned DDL is not one statement or a valid greenfield create set", tc.Table)
	}
	if hasRLS {
		// A new table with row security is created with its policies in one
		// reviewed transaction. Any other row security change on this path
		// is refused: it needs one supported atomic operation.
		creates, err := createsTableWithRowSecurity(tc.DDL)
		if err != nil {
			slog.Warn("PostgreSQL apply admission rejected planned DDL", "database", req.Database, "table", tc.Table, "error", err)
			return nativeApply{}, fmt.Errorf("apply PostgreSQL table %q: classify the planned row security DDL: %w", tc.Table, err)
		}
		if !creates {
			return nativeApply{}, fmt.Errorf("apply PostgreSQL table %q: row security changes require one supported atomic operation for a single qualified table", tc.Table)
		}
		return validateCreateWithRowSecurityApply(req)
	}
	if req.Options["defer_cutover"] == "true" {
		return nativeApply{}, fmt.Errorf("apply PostgreSQL table %q: deferred cutover is unsupported", tc.Table)
	}
	// The same tier derivation runs again inside the background apply; this
	// synchronous check exists so a statement shape the native-safe path
	// cannot execute is refused at acceptance, before any work is queued.
	statements, err := postgresCreateSetStatements(tc.DDL)
	if err != nil {
		slog.Warn("PostgreSQL apply admission rejected planned DDL", "database", req.Database, "table", tc.Table, "error", err)
		return nativeApply{}, fmt.Errorf("apply PostgreSQL table %q: planned DDL is not one statement or a valid greenfield create set", tc.Table)
	}
	// A table drop is not a shape pg-sprite's migrate path executes, so it is
	// admitted on its own terms, against the definition the plan recorded.
	drop, isTableDrop, err := tableDropForApply(req.Database, req.Changes[0].Namespace, tc, statements)
	if err != nil {
		return nativeApply{}, err
	}
	if isTableDrop {
		return drop, nil
	}
	// A label addition to an enum type is the one shape that names a type
	// rather than a table, so it is admitted on its own terms before the
	// table privilege tiers, which have no rung for it.
	enumStatement, enumValueAdd, err := isEnumValueAdd(statements)
	if err != nil {
		slog.Warn("PostgreSQL apply admission rejected planned DDL", "database", req.Database, "table", tc.Table, "error", err)
		return nativeApply{}, fmt.Errorf("apply PostgreSQL table %q: planned statement could not be classified", tc.Table)
	}
	if enumValueAdd {
		return validateEnumValueAdd(req, tc, enumStatement)
	}
	if _, err := preflight.RequiredTier(statements); err != nil {
		return nativeApply{}, fmt.Errorf("apply PostgreSQL table %q: SchemaBot's PostgreSQL support does not execute this statement shape yet", tc.Table)
	}
	if err := plannedSchemaMatchesTarget(statements, req.Changes[0].Namespace); err != nil {
		return nativeApply{}, fmt.Errorf("apply PostgreSQL table %q: %w", tc.Table, err)
	}
	change := nativeApply{database: req.Database, namespace: req.Changes[0].Namespace, table: tc.Table, sql: tc.DDL, steps: len(statements)}
	if len(statements) == 1 {
		// The tier derivation above parsed this statement already, so a
		// parse failure here is an invariant guard; the message stays curated
		// like the refusals above because it reaches the operator surface.
		build, err := classifyIndexBuild(statements[0])
		if err != nil {
			return nativeApply{}, fmt.Errorf("apply PostgreSQL table %q: planned statement could not be classified", tc.Table)
		}
		change.concurrentIndex = build == indexBuildConcurrent
		change.partitionedIndex = build == indexBuildPartitioned
		change.dropped, err = reviewedDropForApply(req.Database, tc, statements[0])
		if err != nil {
			return nativeApply{}, err
		}
		change.indexDrop, err = indexDropForApply(req.Database, change.namespace, tc.Table, statements[0], change.dropped)
		if err != nil {
			return nativeApply{}, err
		}
		change.validates, err = reviewedValidationForApply(req.Database, tc, statements[0])
		if err != nil {
			return nativeApply{}, err
		}
		change.adopts, err = reviewedAdoptionForApply(req.Database, tc, statements[0])
		if err != nil {
			return nativeApply{}, err
		}
		change.replacement, err = indexReplacementForApply(req.Database, tc, change.dropped)
		if err != nil {
			return nativeApply{}, err
		}
	}
	return change, nil
}

// reviewedValidationForApply returns the plan's record of the constraint a
// VALIDATE CONSTRAINT statement validates, and nil for any other statement.
// A validation is admitted only with that record: without it the apply could
// not tell the reviewed constraint from one that took its name after the
// plan, so a missing or unreadable record refuses the apply before any work
// is queued and the operator re-plans.
func reviewedValidationForApply(database string, tc engine.TableChange, statement string) (*schemadiff.ConstraintValidation, error) {
	validates, err := validatesConstraint(statement)
	if err != nil {
		slog.Warn("PostgreSQL apply admission could not classify a planned validation", "database", database, "table", tc.Table, "error", err)
		return nil, fmt.Errorf("apply PostgreSQL table %q: planned statement could not be classified", tc.Table)
	}
	if !validates {
		return nil, nil
	}
	reviewed, err := reviewedValidationFromMetadata(tc.Metadata)
	if err != nil {
		slog.Warn("PostgreSQL apply admission rejected the reviewed validation record", "database", database, "table", tc.Table, "error", err)
		return nil, fmt.Errorf("apply PostgreSQL table %q: the plan's record of the constraint this statement validates is unreadable; re-plan and apply again", tc.Table)
	}
	if reviewed == nil {
		slog.Warn("PostgreSQL apply admission refused a validation the plan did not record", "database", database, "table", tc.Table)
		return nil, fmt.Errorf("apply PostgreSQL table %q: the plan has no record of the constraint this statement validates; re-plan and apply again", tc.Table)
	}
	return reviewed, nil
}

// reviewedDropForApply returns the plan's record of the object a statement
// drops. A statement that drops a live object is admitted only with that
// record: without it the apply could not tell the reviewed object from one
// that took its name or changed its definition after the plan, so it is
// refused before any work is queued and the operator re-plans.
func reviewedDropForApply(database string, tc engine.TableChange, statement string) (*schemadiff.DroppedObject, error) {
	drops, err := dropsLiveObject(statement)
	if err != nil {
		slog.Warn("PostgreSQL apply admission could not classify a planned drop", "database", database, "table", tc.Table, "error", err)
		return nil, fmt.Errorf("apply PostgreSQL table %q: planned statement could not be classified", tc.Table)
	}
	if !drops {
		return nil, nil
	}
	dropped, err := reviewedDropFromMetadata(tc.Metadata)
	if err != nil {
		slog.Warn("PostgreSQL apply admission rejected the reviewed drop record", "database", database, "table", tc.Table, "error", err)
		return nil, fmt.Errorf("apply PostgreSQL table %q: the plan's record of the object this statement drops is unreadable; re-plan and apply again", tc.Table)
	}
	if dropped == nil {
		slog.Warn("PostgreSQL apply admission refused a drop the plan did not record", "database", database, "table", tc.Table)
		return nil, fmt.Errorf("apply PostgreSQL table %q: the plan does not record the object this statement drops, so the apply cannot check it is still the object that was reviewed; re-plan and apply again", tc.Table)
	}
	return dropped, nil
}

// indexBuild is the executor a lone planned statement is admitted to.
type indexBuild int

const (
	// indexBuildNone is every statement that is not a CREATE INDEX.
	indexBuildNone indexBuild = iota
	// indexBuildConcurrent is a CREATE INDEX CONCURRENTLY, which the
	// planner writes for an index on an existing table.
	indexBuildConcurrent
	// indexBuildPartitioned is a CREATE INDEX without CONCURRENTLY. Alone,
	// on an existing table, that statement reaches the apply only as the
	// planner's partitioned index build: an index on a plain table is
	// rewritten to the concurrent form or blocked at plan time, and an index
	// declared with a new table arrives inside its create set. The apply
	// still proves the live table is partitioned before building, so the
	// statement never runs as written.
	indexBuildPartitioned
)

// classifyIndexBuild says which index build executor, if any, runs sql.
func classifyIndexBuild(sql string) (indexBuild, error) {
	statement, err := pgstatement.ParseOne(sql)
	if err != nil {
		return indexBuildNone, fmt.Errorf("parse PostgreSQL statement: %w", err)
	}
	switch {
	case statement.Kind() != pgstatement.KindCreateIndex:
		return indexBuildNone, nil
	case statement.Concurrent():
		return indexBuildConcurrent, nil
	default:
		return indexBuildPartitioned, nil
	}
}

// plannedSchemaMatchesTarget refuses an apply whose planned DDL names a
// different schema than the one the request addresses. The plan qualified
// every statement with the schema it was made against and the apply preflights
// the request's namespace, so the two agree unless the route to the physical
// schema changed after planning; executing then would preflight one schema and
// alter another. An unqualified statement carries no claim to check.
func plannedSchemaMatchesTarget(statements []string, namespace string) error {
	for i, sql := range statements {
		statement, err := pgstatement.ParseOne(sql)
		if err != nil {
			return fmt.Errorf("parse planned statement %d: %w", i+1, err)
		}
		if planned := statement.Schema(); planned != "" && planned != namespace {
			return fmt.Errorf("planned statement %d names schema %q but the apply targets schema %q; the target's schema route changed after planning, so plan again", i+1, planned, namespace)
		}
	}
	return nil
}

// postgresCreateSetStatements parses one statement or a greenfield create set
// using the PostgreSQL parser and returns its canonical execution sequence.
func postgresCreateSetStatements(script string) ([]string, error) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	if err != nil {
		return nil, fmt.Errorf("select PostgreSQL statement parser: %w", err)
	}
	statements, err := ddl.CreateSetStatements(parser, script)
	if err != nil {
		return nil, fmt.Errorf("admit PostgreSQL DDL script: %w", err)
	}
	return statements, nil
}

func (e *Engine) runOptimisticApply(ctx context.Context, conn targetConn, change nativeApply, key string, started time.Time, logger *slog.Logger, tracker *progress.Tracker) {
	// The invalid-index cleanup and the published outcome both classify the
	// cancelled build. Once a Cancel is read as the operator's, it stays so,
	// and the second read does not wait for in-flight calls again.
	var operatorCancel atomic.Bool
	cancelRequested := func() bool {
		if !operatorCancel.Load() && e.cancelRequested(key) {
			operatorCancel.Store(true)
		}
		return operatorCancel.Load()
	}
	// The context arrives detached from the caller (an accepted apply must
	// survive the request) and ends only when the engine cancels the drive,
	// so boundedness comes from the ceiling instead.
	ceiling := e.optimisticApplyCeiling
	if change.durationBoundedIndexBuild() {
		ceiling = e.concurrentIndexMaxDuration + concurrentIndexHeadroom
		change.concurrentIndexMaxDuration = e.concurrentIndexMaxDuration
		change.cancelRequested = cancelRequested
	}
	if change.durationBoundedValidation() {
		// The ceiling leaves room for the setup and the lock retries ahead of
		// the scan, so the server's timer ends an over-long scan first and
		// the outcome names the option.
		ceiling = validationCeiling(e.validateMaxDuration)
		change.validateMaxDuration = e.validateMaxDuration
	}
	ctx, cancel := context.WithTimeout(ctx, ceiling)
	defer cancel()
	// Every terminal result carries the executor's final position: the
	// executor finishes its tracker before it returns, so the read is
	// memory-only and a failed create reports the step that failed, not the
	// sequence's first step. The error branch is an invariant guard, not an
	// expected outcome — a tracker still reporting a live build here means
	// the executor returned without finishing it, and the terminal result
	// then carries the tracker's last-known position instead of a fresh
	// read the reserved session can no longer answer.
	publish := func(result *engine.ProgressResult) {
		if err := executorProgressMetadata(ctx, tracker, result); err != nil {
			logger.Warn("PostgreSQL apply terminal progress reports the last-known executor position",
				"namespace", change.namespace, "table", change.table, "task_id", key, "error", err)
		}
		e.publishProgress(key, result, logger)
	}
	execute := e.execute
	if execute == nil {
		execute = func(ctx context.Context, conn targetConn, change nativeApply, tableSizeLimit int64, tracker *progress.Tracker, logger *slog.Logger) error {
			return executeOptimistic(ctx, conn, change, tableSizeLimit, tracker, logger, e.tableOwner)
		}
	}
	err := execute(ctx, conn, change, e.tableSizeLimit, tracker, logger)
	if err == nil {
		publish(progressResult(engine.StateCompleted, "completed", started, change, ""))
		return
	}

	var invalidErr *executor.InvalidIndexError
	if isCancellation(err) && cancelRequested() {
		// The cancellation is the operator's only when this engine recorded
		// that it acted on the apply: a backend cancelled from outside
		// SchemaBot is a failure the next drive retries, never a cancel
		// nobody asked for.
		if change.partitionedIndex {
			// The partitioned build keeps every partition it finished and
			// the parent's invalid index, and the next apply of the same plan
			// resumes from them, recovering the cancelled partition's
			// leftover first. Nothing is dropped, so there is no cleanup
			// to report.
			publish(progressResult(engine.StateCancelled, "cancelled", started, change, partitionedIndexCancelledDetail))
			return
		}
		detail := cancelledIndexCleanupDetail(err)
		if detail == "" && errors.As(err, &invalidErr) {
			detail = invalidIndexDetail(invalidErr)
		}
		if detail == "" {
			detail = "Concurrent index build cancelled"
		}
		publish(progressResult(engine.StateCancelled, "cancelled", started, change, detail))
		return
	}
	if errors.As(err, &invalidErr) && !invalidErr.Code().Permanent() {
		// An invalid index a retry recovers or an operator clears — a build's
		// own leftover, abandoned debris, another backend's build to wait
		// out, or a builder this role cannot observe — is operational: once
		// it is gone a retry can succeed. Checked before the refusal and
		// budget arms because the verdict wraps the build failure that
		// produced it (a budget-cancelled build leaves its own invalid
		// index), and that inner cause must not be read as the outcome — the
		// index the retry acts on is. The permanent verdicts (the name is occupied
		// on another table, or by an index the server will not drop
		// concurrently) fall through to classifyRefusal: retrying unchanged
		// reproduces them. The detail is built from the typed identifiers
		// and verdict code, never the wrapped build or cleanup errors, which
		// may carry raw server text; the full cause lands in the server log
		// below it.
		logger.Error("PostgreSQL concurrent index build left or found an invalid index",
			"namespace", change.namespace, "table", change.table,
			"index_schema", invalidErr.Schema, "index", invalidErr.Index, "error", err)
		result := progressResult(engine.StateFailed, "failed", started, change,
			invalidIndexDetail(invalidErr))
		result.Retryable = true
		publish(result)
		return
	}

	if r := classifyRefusal(err, change.table); r != nil {
		// The taxonomy's reason survives in the operator-facing detail; no
		// metadata carries it because nothing downstream consumes one yet.
		// The server error is what identifies the occupant or the missing
		// grant, and only the log carries it: the detail is kept generic
		// because it is published to GitHub.
		logger.Warn("PostgreSQL schema change refused",
			append([]any{"namespace", change.namespace, "table", change.table, "reason", r.reason, "error", err}, catalogErrorLogAttrs(err)...)...)
		publish(progressResult(engine.StateFailed, "refused", started, change, recoveryContextDetail(err, r.detail)))
		return
	}

	failure := classifyApplyFailure(err, change.table, change.validates)
	failure.detail = recoveryContextDetail(err, failure.detail)
	switch {
	case failure.committedPrefix:
		logger.Error("PostgreSQL create set failed after committing a prefix",
			"namespace", change.namespace, "table", change.table, "error", err)
	case failure.lockBudget:
		logger.Warn("PostgreSQL native-safe schema change lost its lock budget; the drive will retry",
			"namespace", change.namespace, "table", change.table, "error", err)
	default:
		logger.Error("PostgreSQL native-safe schema change failed",
			append([]any{"namespace", change.namespace, "table", change.table, "error", err}, catalogErrorLogAttrs(err)...)...)
	}
	result := progressResult(engine.StateFailed, "failed", started, change, failure.detail)
	result.Retryable = failure.retryable
	publish(result)
}

// applyFailure is the drive-facing disposition of an operational apply
// failure: the operator detail and whether a retry can succeed. Refusals never
// reach it — classifyRefusal consumes them first.
type applyFailure struct {
	detail          string
	retryable       bool
	committedPrefix bool
	lockBudget      bool
}

// classifyApplyFailure decides retryability from the failure's shape, in the
// order the shapes nest: a create sequence that failed after its CREATE TABLE
// committed is never retryable, even when the failing step lost a lock budget
// that would otherwise be retryable — the table exists, so a retry can only
// collide. classifyRefusal already consumed the statement-budget cause, so a
// budget exceeded here is the lock budget: the statement is native-safe and
// merely lost a bounded race with concurrent lock holders, and marking it
// blocked would falsely tell the operator retrying cannot succeed. validates
// is the plan's record of the constraint the failed step validates, or nil
// when the step validates none.
func classifyApplyFailure(err error, table string, validates *schemadiff.ConstraintValidation) applyFailure {
	if detail, committed := committedCreatePrefixDetail(err, table); committed {
		return applyFailure{detail: detail, committedPrefix: true}
	}
	var partitionedBoundErr *partitionedIndexBoundError
	if errors.As(err, &partitionedBoundErr) {
		// A partitioned build keeps the partitions it finished, so a run
		// that finished at least one (a run that finished none is refused
		// above) spends its retry on the partitions left rather than
		// starting over.
		return applyFailure{detail: partitionedBoundErr.Error() + "; the retry resumes from the partitions already built", retryable: true}
	}
	var budgetErr *executor.BudgetError
	if errors.As(err, &budgetErr) {
		return applyFailure{
			detail:     budgetErr.Error() + "; retry once lock contention subsides",
			retryable:  true,
			lockBudget: true,
		}
	}
	// Rows that break a constraint keep breaking it until someone changes
	// them, which takes a person far longer than the drive's retries do, so
	// the failure ends the apply at once rather than spending its retries on
	// an answer that cannot change. A constraint the apply added is then
	// dropped, so the re-apply after the data fix plans it again; one the
	// table already had stays, and the re-apply resumes at its validation.
	if detail, ok := brokenForeignKeyDetail(err, table); ok {
		return applyFailure{detail: detail}
	}
	if detail, ok := brokenCheckDetail(err, table, validates); ok {
		return applyFailure{detail: detail}
	}
	if detail, ok := duplicateKeyDetail(err, table); ok {
		return applyFailure{detail: detail}
	}
	// An object that depends on what the statement drops keeps depending on
	// it until someone changes that object, so a retry meets the same
	// refusal.
	if detail, ok := dependentObjectsDetail(err, table); ok {
		return applyFailure{detail: detail}
	}
	if detail, ok := missingGrantDetail(err, table); ok {
		return applyFailure{detail: detail, retryable: true}
	}
	return applyFailure{detail: "PostgreSQL schema change failed; see server logs", retryable: true}
}

// missingGrantDetail names a statement the server refused because the engine
// role lacks a privilege. The privilege check before the apply covers the
// target table; a foreign key also needs REFERENCES on the table it
// references, which only the server checks. The detail leaves out the
// server's message, which names objects the plan did not, and the server log
// keeps it.
func missingGrantDetail(err error, table string) (string, bool) {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != sqlstateInsufficientPrivilege {
		return "", false
	}
	return sanitizeReasonText(fmt.Sprintf(
		"the engine role lacks a privilege the statement on %q needs, such as REFERENCES on the table a foreign key references; the server log names it; grant it, then retry",
		table)), true
}

// catalogErrorLogAttrs returns the server's DETAIL and HINT for a refusal
// whose detail names catalog objects rather than row values: the objects
// that depend on what a statement drops, or the privilege a statement
// lacks. The operator-facing detail cuts a long list of dependents and
// leaves the privilege out, so the log carries them whole. Other SQLSTATEs
// return nothing, since a constraint violation's DETAIL quotes the rows'
// values.
func catalogErrorLogAttrs(err error) []any {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return nil
	}
	switch pgErr.Code {
	case sqlstateDependentObjectsStillExist, sqlstateInsufficientPrivilege:
		var attrs []any
		if pgErr.Detail != "" {
			attrs = append(attrs, "server_detail", pgErr.Detail)
		}
		if pgErr.Hint != "" {
			attrs = append(attrs, "server_hint", pgErr.Hint)
		}
		return attrs
	default:
		return nil
	}
}

// sqlstateDependentObjectsStillExist is the SQLSTATE the server raises when
// a statement would drop something other objects depend on, such as a
// column a publication's column list or row filter names. A retype that a
// dependent blocks raises a different, generic SQLSTATE, so it is not
// classified here and keeps the retryable default.
const sqlstateDependentObjectsStillExist = "2BP01"

// maxDependentObjectsDetailLen bounds the server's list of dependent objects
// carried in an apply's failure detail. The server lists every dependent, so
// the list is cut on a rune boundary rather than trusted to fit a status line.
const maxDependentObjectsDetailLen = 300

// dependentObjectsDetail names a statement the server refused because other
// objects depend on what it drops. The plan refuses the
// dependents it can read before the apply starts; this covers the ones only
// the server sees. The server's DETAIL names those objects and nothing else,
// no row values, so the detail carries it: the owner of a hosted deployment
// cannot read the server log, and the dependents are what they have to
// change.
func dependentObjectsDetail(err error, table string) (string, bool) {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != sqlstateDependentObjectsStillExist {
		return "", false
	}
	// The server puts each dependent on its own line; joining them with a
	// comma keeps two of them from reading as one sentence.
	dependents := singleLine(strings.Join(strings.FieldsFunc(pgErr.Detail, func(r rune) bool { return r == '\n' }), ", "))
	if dependents == "" {
		return sanitizeReasonText(fmt.Sprintf(
			"other objects depend on what the statement on %q drops, such as a publication that names the column; the server log names them; remove the dependency, then apply again",
			table)), true
	}
	if runes := []rune(dependents); len(runes) > maxDependentObjectsDetailLen {
		dependents = string(runes[:maxDependentObjectsDetailLen-1]) + "…"
	}
	return sanitizeReasonText(fmt.Sprintf(
		"other objects depend on what the statement on %q drops: %s; remove the dependency, then apply again",
		table, dependents)), true
}

// sqlstateForeignKeyViolation is the SQLSTATE the server raises when a row
// breaks a foreign key, which for a schema change means validating a key
// that existing rows do not satisfy.
const sqlstateForeignKeyViolation = "23503"

// brokenForeignKeyDetail names the foreign key whose validation failed on
// existing rows. A new apply finishes it once the owner fixes the rows. The
// drive removes the key when this apply added it NOT VALID, and leaves one
// that was already on the table, so the detail does not say the key stays:
// the apply log says what became of it. The detail is built from the server's
// typed fields, never its message, because the message quotes the offending
// row's values.
func brokenForeignKeyDetail(err error, table string) (string, bool) {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != sqlstateForeignKeyViolation {
		return "", false
	}
	return sanitizeReasonText(fmt.Sprintf(
		"existing rows in %q break foreign key %q; fix those rows, then apply again",
		table, pgErr.ConstraintName)), true
}

// sqlstateCheckViolation is the SQLSTATE the server raises when a row breaks
// a check constraint, which for a validation step means existing rows do not
// satisfy the constraint the plan added NOT VALID.
const sqlstateCheckViolation = "23514"

// brokenCheckDetail names the check constraint whose validation failed on
// existing rows. It applies only when the failed step validates the
// constraint the server names. A new apply finishes it once the owner fixes
// the rows. For the helper check a SET NOT NULL validates, those are the rows
// that hold NULL in the column, and the drive removes the helper when this
// apply added it, so the detail does not say the constraint stays: the apply
// log says what became of it. The detail is built from the server's typed
// fields, never its message, because the message can quote row values.
func brokenCheckDetail(err error, table string, validates *schemadiff.ConstraintValidation) (string, bool) {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != sqlstateCheckViolation {
		return "", false
	}
	if validates == nil || pgErr.ConstraintName != validates.Constraint {
		return "", false
	}
	return sanitizeReasonText(fmt.Sprintf(
		"existing rows in %q break check constraint %q; fix those rows, then apply again",
		table, pgErr.ConstraintName)), true
}

// sqlstateUniqueViolation is the SQLSTATE the server raises when rows hold
// duplicate values a unique key does not allow, which for a schema change
// means adding a unique constraint that existing rows break.
const sqlstateUniqueViolation = "23505"

// duplicateKeyDetail names the unique key that existing rows break. The
// statement that adds the key runs in one transaction, so it leaves nothing
// behind, and a new apply adds the key once the owner removes or merges the
// duplicates. The detail is built from the server's typed fields, never its
// message or detail, because those quote the duplicated values.
func duplicateKeyDetail(err error, table string) (string, bool) {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != sqlstateUniqueViolation {
		return "", false
	}
	return sanitizeReasonText(fmt.Sprintf(
		"existing rows in %q hold duplicate values that unique key %q does not allow; remove or merge the duplicates, then apply again",
		table, pgErr.ConstraintName)), true
}

// invalidIndexAdvice renders the operator-facing cause and next step for an
// invalid-index verdict, matching pg-sprite's own ownership standard: a
// removal is named only where the recovery proves the entry is a failed
// build's debris on the target table before it drops — this build's own
// leftover, an abandoned entry with no builder, or an entry whose builder
// this role cannot see, which the recovery settles under the table's lock —
// and there the retry performs it, so the operator's step is to let the
// retry run. A build still in flight says wait; an entry on another table,
// one the server will not drop concurrently, and an unproven verdict get
// investigation steps, never a statement to run — the index under the name
// may be healthy or may be exactly what it is meant to be. Only the typed
// identifiers are interpolated, never the wrapped build or cleanup errors,
// which may carry raw server text, and they are rendered inside the room the
// narrowest operator surface leaves them once the prose, the step clause the
// detail will carry, and the remedy's lead have taken theirs, so the lead
// survives that surface for identifiers of any legal length and spelling.
// The two halves leave unsanitized so classifyRefusal can compose a
// sequence-step clause between them and sanitize the whole; the operational
// path composes them through invalidIndexDetail.
func invalidIndexAdvice(invalidErr *executor.InvalidIndexError, stepClause string) (cause, remedy string) {
	w := invalidIndexWordingFor(invalidErr)
	names := invalidIndexNames(invalidErr, w.namesTable, w.namesRoom(stepClause))
	return w.causeBefore + names + w.causeAfter, w.lead + w.remedyRest
}

// invalidIndexWording is the operator-facing prose of one invalid-index
// verdict: the cause on either side of the identifiers it names, and the
// remedy split at its lead — the first clause, the verb and its object,
// which the narrowest operator surface must still show once the detail is
// cut from the tail. Keeping the lead apart is what lets the identifiers be
// given the room that surface leaves rather than a guess at it.
type invalidIndexWording struct {
	causeBefore, causeAfter string
	// namesTable marks a cause that names the table the entry sits on,
	// beside the index name, because that table is where the operator acts.
	namesTable bool
	lead       string
	remedyRest string
}

// namesRoom is the byte room the wording leaves the identifiers inside the
// narrowest operator surface's clamp: what remains of the kept width once
// the cause's prose, the step clause the detail will carry with its
// separator, and the remedy's lead with the separator before it have taken
// theirs.
func (w invalidIndexWording) namesRoom(stepClause string) int {
	room := apitypes.StatusFailureReasonKeptWidth -
		len(w.causeBefore) - len(w.causeAfter) - len(clauseSeparator) - len(w.lead)
	if stepClause != "" {
		room -= len(clauseSeparator) + len(stepClause)
	}
	return room
}

// invalidIndexRetryRemovesRemedy is the remedy for the verdicts whose
// entry the retry itself removes and rebuilds.
const (
	invalidIndexRetryRemovesLead   = "the retry removes it"
	invalidIndexRetryRemovesRemedy = " and rebuilds the index; drop it yourself only if the retries are exhausted"
)

func invalidIndexWordingFor(invalidErr *executor.InvalidIndexError) invalidIndexWording {
	switch invalidErr.Code() {
	case executor.CodeInvalidIndexOwnLeftover:
		// A leftover the bound's statement timeout produced says so: the
		// retry rebuilds under the same bound, so the operator's step is to
		// raise it, and the budget the verdict carries is that bound.
		var budgetErr *executor.BudgetError
		if errors.As(invalidErr.Build, &budgetErr) && budgetErr.Cause == executor.CauseStatement {
			return invalidIndexWording{
				causeBefore: "this build left its own invalid index ",
				causeAfter:  fmt.Sprintf(" on the target after running past %s (%s)", concurrentIndexBoundOption, budgetErr.Budget),
				lead:        invalidIndexRetryRemovesLead,
				remedyRest: fmt.Sprintf(" and rebuilds the index under the same bound, so raise %s first; drop it yourself only if the retries are exhausted",
					concurrentIndexBoundOption),
			}
		}
		return invalidIndexWording{
			causeBefore: "this build left its own invalid index ",
			causeAfter:  " on the target",
			lead:        invalidIndexRetryRemovesLead,
			remedyRest:  invalidIndexRetryRemovesRemedy,
		}
	case executor.CodeInvalidIndexAbandoned:
		return invalidIndexWording{
			causeBefore: "an abandoned invalid index ",
			causeAfter:  " occupies the name on the target table with no backend building it",
			lead:        invalidIndexRetryRemovesLead,
			remedyRest:  invalidIndexRetryRemovesRemedy,
		}
	case executor.CodeInvalidIndexBuildInFlight:
		return invalidIndexWording{
			causeBefore: "an invalid index ",
			causeAfter:  fmt.Sprintf(" occupies the name and backend %d is still building it", invalidErr.BuilderPID),
			lead:        "wait for that build to finish",
			remedyRest:  " or fail, then retry",
		}
	case executor.CodeInvalidIndexBuilderUnobservable:
		return invalidIndexWording{
			causeBefore: "an invalid index ",
			causeAfter:  " occupies the name on the target table with a builder the engine role cannot observe",
			lead:        "the retry proves it abandoned under the table's lock",
			remedyRest:  ", removes it, and rebuilds the index; a build still holding the table stops that retry at its lock budget instead",
		}
	case executor.CodeInvalidIndexOtherTable:
		return invalidIndexWording{
			causeBefore: "an invalid index ",
			causeAfter:  " already occupies the name on a different table",
			namesTable:  invalidErr.Table != "",
			lead:        "rename the index in the schema file",
			remedyRest:  " and re-plan, or clear the entry through that table's own change; this change cannot claim it",
		}
	case executor.CodeInvalidIndexNotDroppable:
		// The kinds of index the server will not drop concurrently are
		// named after the lead, not in the cause: they are why an operator
		// is needed, and the cause has to leave the lead its room.
		return invalidIndexWording{
			causeBefore: "an invalid index ",
			causeAfter:  " occupies the name and is not a failed build's leftover",
			lead:        "an operator must resolve it on the target",
			remedyRest:  ", since it is a partitioned table's index, an index partition, or a constraint's index, or rename the index in the schema file and re-plan",
		}
	case executor.CodeDataFault:
		// The build failed on the table's rows: duplicate values under a
		// unique index, or a value an index expression cannot take. A rerun
		// meets the same rows, so the rows come first. The rerun's recovery
		// then removes the invalid index only once pg-sprite proves no build
		// holds it, so the operator is never sent to drop it by name. The
		// server's error is not rendered: it quotes the offending values.
		causeAfter := ", and the failed build left it invalid on the target"
		if !errors.Is(invalidErr.Cleanup, executor.ErrBuildLeftInvalidIndex) {
			causeAfter = ", and an invalid index this build could not prove its own holds the name"
		}
		return invalidIndexWording{
			causeBefore: "the table's rows cannot be indexed by index ",
			causeAfter:  causeAfter,
			lead:        "correct the rows",
			remedyRest:  ", then re-run; the rerun removes the invalid index once it proves no build holds it",
		}
	default:
		// CodeInvalidIndexUnproven and any future verdict fail safe with
		// investigation steps: the index under the name may be healthy.
		return invalidIndexWording{
			causeBefore: "index ",
			causeAfter:  " may be invalid but its catalog state could not be verified",
			lead:        "inspect pg_index.indisvalid on the target before any recovery",
			remedyRest:  ", then retry",
		}
	}
}

// invalidIndexNames renders the identifiers an invalid-index cause carries —
// the index name qualified by its schema and, when the cause names it, the
// table the entry sits on — quoted inside one byte room. When the quoted
// forms do not all fit, the index name is cut last: it is the name the
// remedy tells the operator to rename or resolve. The table is cut next and
// the schema first, each keeping at least its cut mark, so the cause always
// shows where a name stood. A cut costs the operator little — the server
// log beside the failure carries the identifiers whole.
func invalidIndexNames(invalidErr *executor.InvalidIndexError, withTable bool, room int) string {
	const dot, openParen, closeParen = ".", " (", ")"
	schema, index := strconv.Quote(invalidErr.Schema), strconv.Quote(invalidErr.Index)
	table := ""
	if withTable {
		table = openParen + strconv.Quote(invalidErr.Table) + closeParen
	}
	if len(schema)+len(dot)+len(index)+len(table) <= room {
		return schema + dot + index + table
	}
	reserved := len(dot) + quotedCutWidth
	if withTable {
		reserved += len(openParen) + quotedCutWidth + len(closeParen)
	}
	index = boundedQuote(invalidErr.Index, room-reserved)
	if withTable {
		table = openParen + boundedQuote(invalidErr.Table, room-len(index)-len(dot)-quotedCutWidth-len(openParen)-len(closeParen)) + closeParen
	}
	schema = boundedQuote(invalidErr.Schema, room-len(index)-len(dot)-len(table))
	return schema + dot + index + table
}

// invalidIndexDetail composes the advice for the operational (retryable)
// publish path, where no sequence-step clause intervenes.
func invalidIndexDetail(invalidErr *executor.InvalidIndexError) string {
	cause, remedy := invalidIndexAdvice(invalidErr, "")
	return sanitizeReasonText(cause + clauseSeparator + remedy)
}

// refusal is a typed apply outcome that retrying cannot fix: the schema
// change, the target table, or role provisioning must change first.
//
// cause names what failed and remedy names what the operator changes; they
// are kept apart so the failed sequence step can be placed between them
// without parsing the rendered text. detail is the operator-facing line
// classifyRefusal composes from them — the only field consumers read.
//
// provisionable marks a refusal whose remedy is something the operator adds
// to the target — a grant, a role, a schema — with the plan left as it is.
// The plan-time gates use it to decide whether a refusal earns a place beside
// a verdict another gate already gave: a provisionable one names work the
// operator will have to do anyway, while a refusal about the table itself
// (vanished, not a table) names nothing to provision and only a re-plan can
// answer it. It is set by the arms that know their remedy is provisioning
// and stays false for every other refusal, so a new refusal defaults to the
// conservative side.
type refusal struct {
	reason        string
	cause         string
	remedy        string
	detail        string
	provisionable bool
}

// classifyRefusal maps pg-sprite's typed refusal inputs to permanent
// refusals, for both the plan-time privilege check and the apply path — one
// classifier so the same underlying failure carries the same reason and
// detail on both surfaces; the plan surface prefixes the detail with the
// statement it blocks, the apply surface publishes it bare. A nil result
// means the failure is operational — a retry may succeed once conditions
// change. Lock-budget exhaustion is deliberately
// operational: the statement is native-safe and only lost a bounded race
// with concurrent lock holders. Every cause and remedy is built from typed
// error fields and identifiers, never from wrapped server output, and the
// composed detail is sanitized at this single exit — a refusal is safe to
// render on operator-facing surfaces by construction, whichever branch
// produced it.
//
// The detail reads cause, then the failed sequence step when the statement
// was one of several, then the remedy, so the remedy stays the last clause
// the operator reads. A failure past the first step leaves the CREATE TABLE
// committed; a refusal that carries no remedy of its own then gets a re-plan,
// because the plan that produced the sequence no longer matches the target.
func classifyRefusal(err error, table string) *refusal {
	r := refusalForCause(err, table)
	if r == nil {
		return nil
	}
	r.compose(err)
	return r
}

// compose sets the detail from the cause, the failed step of a create set
// when there is one, and the remedy, joined by the clause separator. A
// refusal without a remedy of its own that failed past the committed CREATE
// TABLE is told to re-plan, because the plan that produced the sequence no
// longer matches the target.
func (r *refusal) compose(err error) {
	clauses := []string{r.cause}
	if clause := sequenceStepClause(err); clause != "" {
		clauses = append(clauses, clause)
	}
	remedy := r.remedy
	var stepErr *executor.SequenceStepError
	if remedy == "" && errors.As(err, &stepErr) && stepErr.Step > 1 {
		remedy = replanRemedy
	}
	if remedy != "" {
		clauses = append(clauses, remedy)
	}
	r.detail = sanitizeReasonText(strings.Join(clauses, clauseSeparator))
}

const (
	replanRemedy             = "re-plan against the current schema"
	invariantViolationRemedy = "inspect the target and server logs before re-running"
	clauseSeparator          = "; "
)

// sequenceStepClause names the failed step of a multi-statement create set
// and, past the first step, that the CREATE TABLE committed; a statement that
// was not one of several gets no clause. It sits between a cause and a remedy
// and does not repeat the table: the cause before it names the table where
// that matters, and every surface renders the detail beside the table it
// belongs to. Keeping the clause short is what lets the remedy's lead survive
// the narrowest operator surface, which truncates the detail from the tail,
// for a table name of any legal length; a cause that spends byte room on
// identifiers charges the clause's width before the identifiers take theirs.
func sequenceStepClause(err error) string {
	var stepErr *executor.SequenceStepError
	if !errors.As(err, &stepErr) || stepErr.Total <= 1 {
		return ""
	}
	clause := fmt.Sprintf("step %d of %d failed", stepErr.Step, stepErr.Total)
	if stepErr.Step > 1 {
		clause += " after the CREATE TABLE committed"
	}
	return clause
}

// committedCreatePrefixDetail reports the non-retryable recovery action for a
// create sequence that failed after at least one earlier step committed. No
// cause precedes this detail, so it names the table itself.
func committedCreatePrefixDetail(err error, table string) (string, bool) {
	var stepErr *executor.SequenceStepError
	if !errors.As(err, &stepErr) || stepErr.Step <= 1 {
		return "", false
	}
	detail := fmt.Sprintf("step %d of %d failed after the CREATE TABLE for %s committed; %s",
		stepErr.Step, stepErr.Total, quotedTable(table), replanRemedy)
	return sanitizeReasonText(detail), true
}

// privilegeRemedy is the statement that provisions the access a privilege
// refusal names, with the refusal's hint. The catalog check that failed is
// how the engine proved the access missing, not something the reader runs,
// so it is left out.
func privilegeRemedy(err *preflight.PrivilegeError) string {
	remedy := "provision with: " + err.Grant
	if err.Hint != "" {
		remedy += "; " + err.Hint
	}
	return remedy
}

// refusalForCause holds classifyRefusal's cause-to-refusal mapping; causes
// and remedies leave unsanitized and classifyRefusal sanitizes the composed
// detail at its return.
func refusalForCause(err error, table string) *refusal {
	// An enum step's own outcomes are decided first, so a missing grant on
	// the type is named as the type's, not a table's.
	if r := enumTypeRefusal(err, table); r != nil {
		return r
	}
	var privilegeErr *preflight.PrivilegeError
	if errors.As(err, &privilegeErr) {
		object := "on table " + quotedTable(table)
		if privilegeErr.Tier == preflight.TierCreateTable {
			// The create tier's grant is schema-scoped: the table does not
			// exist yet, so pointing the operator at a table-level grant
			// would send them hunting for an object the target lacks.
			object = "in the schema that would hold table " + quotedTable(table)
		}
		return &refusal{reason: "insufficient-privileges",
			cause:         fmt.Sprintf("the engine role lacks access for %s %s", privilegeErr.Tier, object),
			remedy:        privilegeRemedy(privilegeErr),
			provisionable: true}
	}
	// An invalid-index verdict is decided by its own code, never by the build
	// failure it wraps — a budget-cancelled concurrent build leaves its own
	// invalid index, and the index a retry recovers or an operator clears is
	// the outcome, not the inner budget exhaustion. Decided before the budget arm so the
	// nested cause can never shadow the verdict. Only the permanent members
	// of the family refuse: the name is occupied on another table, or by an
	// index the server will not drop concurrently, so retrying unchanged
	// reproduces the verdict. Every other member is operational.
	var invalidErr *executor.InvalidIndexError
	if errors.As(err, &invalidErr) {
		r, _ := refusalForOutcome(invalidErr.Code(), table)
		if r != nil {
			r.cause, r.remedy = invalidIndexAdvice(invalidErr, sequenceStepClause(err))
		}
		return r
	}
	// A concurrent build that ran past its bound and provably left nothing
	// is refused like any other statement exhaustion — retrying unchanged
	// spends the same bound again — but names the option that grants more
	// time, since the bound is the operator's to set. The wrapper is decided
	// on its own: the bound also ends the executor's catalog reads ahead of
	// the build, which carry no typed budget verdict.
	var boundErr *concurrentIndexBoundError
	if errors.As(err, &boundErr) {
		return &refusal{reason: "concurrent-index-bound-exceeded", cause: boundErr.Error(),
			remedy: fmt.Sprintf("raise %s and re-run", concurrentIndexBoundOption)}
	}
	// A validation that ran past its bound is refused: retrying unchanged
	// spends the same bound on the same scan. The constraint stays NOT VALID
	// and checks new writes, and the remedy names the option that grants
	// more time.
	var validationErr *validationBoundError
	if errors.As(err, &validationErr) {
		return &refusal{reason: "validation-bound-exceeded", cause: validationErr.Error(),
			remedy: fmt.Sprintf("raise %s and re-run", validationBoundOption)}
	}
	// A partitioned build that ran past its bound without finishing a
	// partition made no progress a retry could keep, so retrying unchanged
	// spends the same bound on the same partition again. It is refused like
	// the concurrent build's overrun and names the same option.
	var partitionedBoundErr *partitionedIndexBoundError
	if errors.As(err, &partitionedBoundErr) && partitionedBoundErr.partitionsBuilt == 0 {
		return &refusal{reason: "partitioned-index-bound-exceeded", cause: partitionedBoundErr.Error(),
			remedy: fmt.Sprintf("raise %s and re-run", concurrentIndexBoundOption)}
	}
	// A cancelled concurrent drop can leave the index invalid, which no plan
	// drops, so the refusal names the index and the drop to finish by hand.
	// Decided before the budget arm, which would hide the index.
	var dropInterruptedErr *indexDropInterruptedError
	if errors.As(err, &dropInterruptedErr) {
		return indexDropInterruptedRefusal(dropInterruptedErr, table)
	}
	var budgetErr *executor.BudgetError
	if errors.As(err, &budgetErr) && budgetErr.Cause == executor.CauseStatement {
		return &refusal{reason: "not-native-safe-budget-exceeded", cause: budgetErr.Error()}
	}
	var partitionErr *preflight.UnsupportedPartitionedParentError
	if errors.As(err, &partitionErr) {
		// The typed error's message is a fixed English sentence with no
		// interpolated identifiers or server text — a deliberate pg-sprite
		// property — so rendering it verbatim is safe by construction.
		return &refusal{reason: "unsupported-partitioned-parent", cause: partitionErr.Error()}
	}
	var proofErr *catalogOnlyProofError
	if errors.As(err, &proofErr) {
		return catalogOnlyProofRefusal(proofErr, table)
	}
	var sizeErr *preflight.SizeError
	if errors.As(err, &sizeErr) {
		return tableTooLargeRefusal(sizeErr)
	}
	// Decided before the bare table sentinels below: the unverified-create
	// wrap carries the read-back's own cause inside it, and a table no longer
	// at its name or no longer a table are two of those causes. The outcome
	// is the state the step left — a committed table whose names are
	// unproven — not the fault that kept them from being proven, so the
	// inner sentinel must not claim the verdict.
	if errors.Is(err, executor.ErrCreateNamesUnverified) {
		return createNamesUnverifiedRefusal(table)
	}
	if errors.Is(err, executor.ErrCreateOwnerUnverified) {
		return createOwnerUnverifiedRefusal(table)
	}
	if errors.Is(err, preflight.ErrTableNotFound) {
		return tableNotFoundRefusal(table)
	}
	if errors.Is(err, preflight.ErrNotTable) {
		return &refusal{reason: "not-a-table",
			cause: quotedTable(table) + " exists but is not an ordinary or partitioned table"}
	}
	if preflight.IsNameOccupied(err) {
		return createCollisionRefusal(table)
	}
	var mismatchErr *executor.CreateNameMismatchError
	if errors.As(err, &mismatchErr) {
		return createNameMismatchRefusal(createNameMismatchCause(table, mismatchErr, sequenceStepClause(err)))
	}
	var ownerMismatchErr *executor.CreateOwnerMismatchError
	if errors.As(err, &ownerMismatchErr) {
		return createOwnerMismatchRefusal(table, ownerMismatchErr)
	}
	if errors.Is(err, preflight.ErrCreateOwnerNotFound) {
		// The owner is target configuration and the server has no such
		// role: the same environmental class as a missing grant, decided
		// before anything runs, but with no grantee for a GRANT to name.
		return &refusal{reason: "insufficient-privileges",
			cause:         fmt.Sprintf("the configured table owner for %s is not a role on the target", quotedTable(table)),
			remedy:        "create the role or correct the target's table_owner, then re-plan",
			provisionable: true}
	}
	if errors.Is(err, preflight.ErrSchemaNotFound) {
		return &refusal{reason: "schema-not-found",
			cause:         fmt.Sprintf("the schema that would hold table %s does not exist on the target", quotedTable(table)),
			remedy:        "create the schema first",
			provisionable: true}
	}
	r, _ := refusalForOutcome(executor.OutcomeCode(err), table)
	return r
}

// refusalForOutcome maps pg-sprite's stable executor outcome vocabulary to
// apply dispositions: a refusal when retrying the identical plan cannot
// succeed, nil when the outcome is operational and a retry may. The switch
// is total over executor.Codes() — the exhaustiveness test pins every code
// to an explicit decision, so a new pg-sprite outcome surfaces as a failing
// test here instead of silently falling through to the retryable tail. The
// boolean reports whether the code is a known member of the vocabulary.
func refusalForOutcome(code executor.Code, table string) (*refusal, bool) {
	switch code {
	case executor.CodeCreateCollision:
		return createCollisionRefusal(table), true
	case executor.CodeCreateNameMismatch:
		// The code is only ever carried by the typed error that names the
		// relations involved, and refusalForCause decides on that error
		// before control reaches here; this arm keeps the switch total and
		// gives the bare code the same verdict, with a cause that knows only
		// that a name differs.
		return createNameMismatchRefusal(
			fmt.Sprintf(mismatchCauseFormat, quotedTable(table), "a name")), true
	case executor.CodeCreateNamesUnverified:
		// The sentinel is present whenever this code is, so refusalForCause
		// decides the verdict before control reaches here; this arm keeps
		// the switch total and gives the bare code the same verdict.
		return createNamesUnverifiedRefusal(table), true
	case executor.CodeCreateOwnerMismatch:
		// Like the name mismatch, the code is only ever carried by the typed
		// error naming both roles, which refusalForCause decides on first;
		// this arm gives the bare code the same verdict with a cause that
		// knows only that the owner differs.
		return createOwnerMismatchRefusal(table, nil), true
	case executor.CodeCreateOwnerUnverified:
		// The sentinel is present whenever this code is, so refusalForCause
		// decides the verdict before control reaches here; this arm keeps
		// the switch total and gives the bare code the same verdict.
		return createOwnerUnverifiedRefusal(table), true
	case executor.CodeDuplicateCreateName:
		// The cause says only what the set did; the remedy names the
		// relation kinds that can repeat a name — a CREATE INDEX name that
		// is also the table's implicit constraint-index name, or another
		// index's — so the cause stays short enough for the remedy to
		// survive the narrowest surface beside a table name of any length.
		return &refusal{reason: "duplicate-create-name",
			cause:  fmt.Sprintf("the create set for %s claims the same relation name twice", quotedTable(table)),
			remedy: "rename the index or constraint that repeats it in the schema file, then " + replanRemedy}, true
	case executor.CodePartitionOfUnsupported:
		return &refusal{reason: "unsupported-create-step",
			cause: fmt.Sprintf("the CREATE TABLE for %s attaches a partition to a live parent, which the native-safe create path does not run", quotedTable(table))}, true
	case executor.CodeIfNotExistsUnsupported:
		return &refusal{reason: "unsupported-create-step",
			cause:  fmt.Sprintf("the planned statement for %s carries IF NOT EXISTS, whose no-op outcome the native-safe path cannot prove", quotedTable(table)),
			remedy: "drop the clause and re-plan"}, true
	case executor.CodeUnsupportedCreateStep:
		return &refusal{reason: "unsupported-create-step",
			cause:  fmt.Sprintf("the CREATE TABLE for %s is not a shape the native-safe create path can run", quotedTable(table)),
			remedy: "rewrite the schema file and re-plan"}, true
	case executor.CodeTableNotFound:
		return tableNotFoundRefusal(table), true
	case executor.CodeEmptySequence, executor.CodeUnsupportedSequenceStep,
		executor.CodeUnsupportedPartitionedParent, executor.CodeNotConcurrentIndexBuild,
		executor.CodeUnnamedIndex, executor.CodeUnqualifiedTable,
		executor.CodeUnsupportedAcceptedBlocking, executor.CodeNotCreateIndex,
		executor.CodeNotAddKeyConstraint, executor.CodeNotAddForeignKey:
		// Shape refusals: the executor refused the statement's form at
		// admission, so retrying the identical plan refails the same way.
		return &refusal{reason: "unsupported-statement-shape",
			cause:  fmt.Sprintf("the planned statement for %s is not a shape the native-safe path can run", quotedTable(table)),
			remedy: "rewrite the schema change and re-plan"}, true
	case executor.CodeInvalidBlockingBudget:
		// The bound is the engine's configuration, not the statement's
		// form: no edit to a schema file changes it, so the remedy points
		// at the budget instead of at the schema change.
		return &refusal{reason: "invalid-blocking-budget",
			cause:  fmt.Sprintf("the blocking budget for the change to %s is not a bound the engine can enforce", quotedTable(table)),
			remedy: "correct the engine's blocking budget configuration, then re-run"}, true
	case executor.CodeRowSecurityPlanChanged:
		return &refusal{reason: "row-security-plan-changed",
			cause:  fmt.Sprintf("the row security plan for %s changed after review", quotedTable(table)),
			remedy: "re-plan and review the new SQL before applying"}, true
	case executor.CodeRowSecurityRefused:
		// Keep every pg-sprite outcome classified at the adapter boundary.
		return &refusal{reason: "row-security-refused",
			cause:  fmt.Sprintf("the row security change for %s was refused by the engine", quotedTable(table)),
			remedy: "check the declaration, target, and privileges before re-planning"}, true
	case executor.CodeRowSecurityOutcomeUnknown:
		// Keep every pg-sprite outcome classified at the adapter boundary.
		// pg-sprite requires catalog inspection before any retry.
		return &refusal{reason: "row-security-outcome-unknown",
			cause:  fmt.Sprintf("whether the row security change for %s committed is unknown", quotedTable(table)),
			remedy: "inspect the target policies before re-running"}, true
	case executor.CodeBlockingOutcomeUnknown:
		// The statement's commit was sent and its answer never arrived, so
		// whether the change landed is open. The engine leaves the retry
		// decision to its adapter; SchemaBot's retry re-runs the identical
		// statement, which replays a committed blocking DDL onto its own
		// result, so the apply fails closed until an operator has read the
		// catalog.
		return &refusal{reason: "blocking-outcome-unknown",
			cause:  fmt.Sprintf("whether the blocking statement for %s committed is unknown", quotedTable(table)),
			remedy: "inspect the target catalog before re-running"}, true
	case executor.CodeCommitOutcomeUnknown:
		// The bounded attempt's COMMIT did not answer and may have landed;
		// the same retry hazard as the blocking case, so the apply fails
		// closed until an operator has read the catalog.
		return &refusal{reason: "commit-outcome-unknown",
			cause:  fmt.Sprintf("whether the statement for %s committed is unknown", quotedTable(table)),
			remedy: "inspect the target catalog before re-running"}, true
	case executor.CodeReplicaIdentityDrop:
		// Nothing was dropped: the engine re-read the catalog under the
		// table's lock and found the drop would remove what a replica
		// identity index is built on. Retrying unchanged meets the same
		// identity, so the operator has to point it elsewhere first.
		return &refusal{reason: "replica-identity-drop",
			cause:  fmt.Sprintf("the planned drop on %s would remove what its replica identity index is built on", quotedTable(table)),
			remedy: "point REPLICA IDENTITY elsewhere, then " + replanRemedy}, true
	case executor.CodeNotPartitionedTable:
		// The plan routed the index to the partitioned build, and the table
		// is no longer partitioned. The build refuses before it touches the
		// target, so the statement never runs as a blocking CREATE INDEX; a
		// new plan routes the index to the concurrent build instead.
		return &refusal{reason: "not-partitioned-table",
			cause:  fmt.Sprintf("%s is no longer partitioned, so the index planned for its partitions was not built", quotedTable(table)),
			remedy: replanRemedy}, true
	case executor.CodePartitionedIndexNameTaken:
		// The build left the relation holding the name alone, so only an
		// operator freeing the name, or the schema file choosing another,
		// changes the outcome.
		return &refusal{reason: "partitioned-index-name-taken",
			cause:  fmt.Sprintf("a name the index build on partitioned table %s needs is held by another relation", quotedTable(table)),
			remedy: "rename the index in the schema file and re-plan, or drop or rename the relation that holds the name"}, true
	case executor.CodeForeignTablePartition:
		return &refusal{reason: "foreign-table-partition",
			cause:  fmt.Sprintf("a partition of %s is a foreign table, which cannot have an index; nothing was created", quotedTable(table)),
			remedy: "detach the foreign table partition, or leave the index out of the schema file, then " + replanRemedy}, true
	case executor.CodeUniqueIndexPartitionKey:
		return &refusal{reason: "unique-index-partition-key",
			cause:  fmt.Sprintf("the unique index on %s does not include every partition key column", quotedTable(table)),
			remedy: "add the partition key columns to the index in the schema file, then " + replanRemedy}, true
	case executor.CodeTableLockUnconfirmed:
		// The connected role cannot see when other sessions started, which
		// the build needs to prove the table lock it waits behind is its
		// own. The grant is target configuration, so the same environmental
		// class as a missing privilege.
		return &refusal{reason: "insufficient-privileges",
			cause:         fmt.Sprintf("the connected role cannot see other sessions' start times, which the index build on %s needs", quotedTable(table)),
			remedy:        "grant the role pg_read_all_stats, then re-run",
			provisionable: true}, true
	case executor.CodePartitionDetachPending:
		return &refusal{reason: "partition-detach-pending",
			cause:  fmt.Sprintf("a partition of %s has an unfinished concurrent detach", quotedTable(table)),
			remedy: "finish the detach with ALTER TABLE ... DETACH PARTITION ... FINALIZE, then re-run"}, true
	case executor.CodeDetachDefaultPartition:
		// PostgreSQL never detaches a partition concurrently from a table
		// that has a DEFAULT partition; nothing ran, and a retry meets the
		// same DEFAULT partition.
		return &refusal{reason: "detach-default-partition",
			cause:  fmt.Sprintf("%s has a DEFAULT partition, so PostgreSQL will not detach a partition from it concurrently; nothing ran", quotedTable(table)),
			remedy: "detach the partition outside SchemaBot, then " + replanRemedy}, true
	case executor.CodePartitionedKeyNameTaken:
		// The build left the occupant alone, so only an operator freeing the
		// name, or the schema file choosing another, changes the outcome.
		return &refusal{reason: "partitioned-key-name-taken",
			cause:  fmt.Sprintf("a name the key constraint on partitioned table %s needs on a partition is held by something else", quotedTable(table)),
			remedy: "rename the constraint in the schema file and re-plan, or drop or rename what holds the name"}, true
	case executor.CodePartitionedKeyConflict:
		return &refusal{reason: "partitioned-key-conflict",
			cause:  fmt.Sprintf("a partition of %s already has a key constraint that conflicts with the planned one; nothing was added", quotedTable(table)),
			remedy: "drop or change the partition's key, or change the key in the schema file, then " + replanRemedy}, true
	case executor.CodePartitionedKeyNullableColumn:
		return &refusal{reason: "partitioned-key-nullable-column",
			cause:  fmt.Sprintf("the planned primary key on %s covers a column a partition allows to be NULL; nothing was built", quotedTable(table)),
			remedy: "make the column NOT NULL on every partition, then " + replanRemedy}, true
	case executor.CodePartitionedKeyUnsupportedShape:
		return &refusal{reason: "unsupported-statement-shape",
			cause:  fmt.Sprintf("the key on partitioned table %s has no name or a clause its partitions cannot carry", quotedTable(table)),
			remedy: "name the key and drop that clause in the schema file, then " + replanRemedy}, true
	case executor.CodePartitionedKeyParentConflict:
		// Nothing was created, and the parent's own constraint or the
		// relation holding the index name stays until an operator or the
		// schema file changes it.
		return &refusal{reason: "partitioned-key-parent-conflict",
			cause:  fmt.Sprintf("partitioned table %s already has the key's name in use, or a primary key", quotedTable(table)),
			remedy: "rename the key in the schema file, or free the name, then " + replanRemedy}, true
	case executor.CodePartitionedKeySubPartitioned:
		return &refusal{reason: "unsupported-statement-shape",
			cause:  fmt.Sprintf("a partition of %s is itself partitioned, which the key build does not run", quotedTable(table)),
			remedy: "add the key to that partition by hand, then " + replanRemedy}, true
	case executor.CodePartitionedForeignKeyNameTaken:
		// The build left the occupant alone, so only an operator freeing the
		// name, or the schema file choosing another, changes the outcome.
		return &refusal{reason: "partitioned-foreign-key-name-taken",
			cause:  fmt.Sprintf("a name the foreign key on partitioned table %s needs on a partition is held by another constraint", quotedTable(table)),
			remedy: "rename the foreign key in the schema file and re-plan, or drop or rename the constraint that holds the name"}, true
	case executor.CodePartitionedForeignKeyConflict:
		return &refusal{reason: "partitioned-foreign-key-conflict",
			cause:  fmt.Sprintf("a partition of %s has duplicate foreign keys the parent's cannot adopt; nothing was added", quotedTable(table)),
			remedy: "drop the partition's duplicate foreign keys, then " + replanRemedy}, true
	case executor.CodePartitionedForeignKeyReferencedTable:
		return &refusal{reason: "unsupported-statement-shape",
			cause:  fmt.Sprintf("the foreign key on %s must reference a plain table outside its partitions", quotedTable(table)),
			remedy: "reference a plain table outside the partition tree in the schema file, then " + replanRemedy}, true
	case executor.CodePartitionedForeignKeyUnsupportedShape:
		return &refusal{reason: "unsupported-statement-shape",
			cause:  fmt.Sprintf("the foreign key on partitioned table %s has no name or a clause its partitions cannot carry", quotedTable(table)),
			remedy: "name the foreign key and drop that clause in the schema file, then " + replanRemedy}, true
	case executor.CodePartitionedForeignKeyParentConflict:
		// Nothing was created, and the parent's own constraint stays until
		// an operator or the schema file changes it.
		return &refusal{reason: "partitioned-foreign-key-parent-conflict",
			cause:  fmt.Sprintf("partitioned table %s already uses the foreign key's name, or has it under another name", quotedTable(table)),
			remedy: "rename the foreign key in the schema file, or free the name, then " + replanRemedy}, true
	case executor.CodePartitionedForeignKeySubPartitioned:
		return &refusal{reason: "unsupported-statement-shape",
			cause:  fmt.Sprintf("a partition of %s is itself partitioned, which the foreign key build does not run", quotedTable(table)),
			remedy: "add the foreign key to that partition by hand, then " + replanRemedy}, true
	case executor.CodePartitionedForeignKeyNotValidSupported:
		// The server accepts NOT VALID on the partitioned table, so a new
		// plan routes the foreign key to the NOT VALID then VALIDATE steps.
		return &refusal{reason: "partitioned-foreign-key-not-valid-supported",
			cause:  fmt.Sprintf("the server adds a foreign key on partitioned table %s online without the partition build; nothing was created", quotedTable(table)),
			remedy: replanRemedy}, true
	case executor.CodePartitionedIndexTablespace:
		return &refusal{reason: "unsupported-statement-shape",
			cause:  fmt.Sprintf("the index on %s names a TABLESPACE, which the partitions' indexes would not use", quotedTable(table)),
			remedy: "remove the TABLESPACE clause from the schema file, then " + replanRemedy}, true
	case executor.CodeDropOutcomeUnknown:
		// The drop ran, and the check of which index it removed failed.
		// SchemaBot's retry re-runs the drop, which would act on whatever
		// now holds the name, so the apply fails closed until an operator
		// has read the catalog.
		return &refusal{reason: "drop-outcome-unknown",
			cause:  fmt.Sprintf("an index drop on %s ran, and which index it removed could not be confirmed", quotedTable(table)),
			remedy: "inspect the table's indexes before re-running"}, true
	case executor.CodeDroppedIndexReplaced:
		// Unlike the drift codes, something was dropped: another session
		// gave the name to a different index between the check and the drop.
		return &refusal{reason: "dropped-index-replaced",
			cause:  fmt.Sprintf("the index drop on %s removed an index created after review under the reviewed index's name", quotedTable(table)),
			remedy: "inspect the table's indexes and recreate the dropped one if it should stay, then " + replanRemedy}, true
	case executor.CodeDroppedObjectHasDescendants:
		return &refusal{reason: "dropped-object-has-descendants",
			cause:  fmt.Sprintf("the planned drop on %s would also reach unreviewed partitions or child tables", quotedTable(table)),
			remedy: "make the drop outside SchemaBot on the whole table tree, then " + replanRemedy}, true
	case executor.CodeDroppedColumnPartitionKey:
		// PostgreSQL never drops a column in a partition key, read under the
		// table's lock; nothing was dropped.
		return &refusal{reason: "dropped-column-partition-key",
			cause:  fmt.Sprintf("the planned column drop on %s names a column in a partition key, which PostgreSQL cannot drop", quotedTable(table)),
			remedy: "declare the column again in the schema file, then " + replanRemedy}, true
	case executor.CodeDroppedColumnPartitionDependent:
		// A partition has an object of its own on the column, which the
		// drop would remove without review; nothing was dropped.
		return &refusal{reason: "dropped-column-partition-dependent",
			cause:  fmt.Sprintf("the planned column drop on %s would also remove an index, constraint, or other object a partition has on that column", quotedTable(table)),
			remedy: "drop or change that object on the partition, then " + replanRemedy}, true
	case executor.CodeDroppedTableHasParent:
		// The table the plan drops became a partition or an inheritance
		// child after planning, read under its lock; nothing was dropped.
		// Dropping it would remove rows of the table above it.
		return &refusal{reason: "dropped-table-has-parent",
			cause:  fmt.Sprintf("table %s is now a partition or inheritance child", quotedTable(table)),
			remedy: "make the drop outside SchemaBot on the whole table tree, then " + replanRemedy}, true
	case executor.CodeDroppedTableHasDependents:
		// A view or another object depends on the table, and the drop never
		// cascades, so PostgreSQL refused it; nothing was dropped.
		return &refusal{reason: "dropped-table-has-dependents",
			cause:  fmt.Sprintf("a view or other object depends on table %s", quotedTable(table)),
			remedy: "drop or change the objects that depend on the table, then apply again"}, true
	case executor.CodeAdoptedIndexDrift, executor.CodeValidatedConstraintDrift,
		executor.CodeRenamedIndexDrift, executor.CodeDroppedIndexDrift:
		// The index or constraint the planned statement names changed after
		// planning — dropped, replaced, left invalid, or now backing a
		// constraint — read again right before the statement; nothing was
		// applied. The plan is stale, so only a re-plan changes the outcome.
		return &refusal{reason: "planned-object-drift",
			cause:  fmt.Sprintf("the index or constraint the planned statement for %s names changed after planning", quotedTable(table)),
			remedy: replanRemedy}, true
	case executor.CodeReplacementIndexDrift:
		// The index the plan built to replace the one it drops is gone,
		// invalid, or rebuilt with another definition, read again right
		// before the drop; nothing was dropped, so the old index still
		// enforces its definition. A re-plan builds the replacement again.
		return &refusal{reason: "planned-object-drift",
			cause:  fmt.Sprintf("the index the plan built on %s to replace the one it drops changed before the drop, so the old index was kept", quotedTable(table)),
			remedy: replanRemedy}, true
	case executor.CodeDroppedObjectDrift:
		// The table, column, constraint, or NOT NULL the planned drop removes
		// no longer has the definition the plan recorded, read again under the
		// table's lock; nothing was dropped. The plan is stale, so only a
		// re-plan changes the outcome.
		return &refusal{reason: "planned-object-drift",
			cause:  fmt.Sprintf("the object the planned drop on %s removes changed after planning", quotedTable(table)),
			remedy: replanRemedy}, true
	case executor.CodeEnumTypeDrift:
		// The type name an ADD VALUE spells resolved to a different type
		// than the one the engine checked, and the attempt rolled back with
		// nothing added. The plan's view of the type is stale, so only a
		// re-plan changes the outcome.
		return &refusal{reason: "planned-object-drift",
			cause:  fmt.Sprintf("the enum type the planned statement for %s names is a different type than the one checked", quotedTable(table)),
			remedy: replanRemedy}, true
	case executor.CodeEnumLabelExists, executor.CodeEnumNeighborMissing:
		// The enum type gained the label the ADD VALUE adds, or lost the
		// label it is placed next to, after planning, and nothing was added.
		// enumTypeRefusal decides on the typed error first and names the
		// labels; this arm keeps the switch total and gives the bare code the
		// same verdict.
		return &refusal{reason: "planned-object-drift",
			cause:  fmt.Sprintf("the enum type the planned statement for %s names changed its labels after planning", quotedTable(table)),
			remedy: replanRemedy}, true
	case executor.CodeDataFault:
		// The server rejected the table's rows as the statement would shape
		// them (a value the new type will not hold, a constraint the rows
		// break). Any rerun meets the same rows, so the data or the
		// statement has to change first.
		return &refusal{reason: "data-fault",
			cause:  fmt.Sprintf("the rows of %s do not fit the shape the planned statement gives them", quotedTable(table)),
			remedy: "correct the data or the statement, then " + replanRemedy}, true
	case executor.CodeBudgetStatementExceeded:
		// Normally consumed upstream by the typed BudgetError arm, which
		// renders the budget's own figures; this mapping keeps the outcome
		// vocabulary total.
		return &refusal{reason: "not-native-safe-budget-exceeded",
			cause: fmt.Sprintf("the statement for table %s ran past its statement budget and was cancelled", quotedTable(table))}, true
	case executor.CodeInvariantViolation:
		// Never a retry candidate per the executor's contract: an invariant
		// breach means the engine's own safety accounting failed, so the
		// apply fails closed until an operator has inspected the target.
		return &refusal{reason: "engine-invariant-violation",
			cause:  fmt.Sprintf("the engine's safety invariants did not hold while changing table %s", quotedTable(table)),
			remedy: invariantViolationRemedy}, true
	case executor.CodeInvalidIndexOtherTable, executor.CodeInvalidIndexNotDroppable:
		// The permanent members of the invalid-index family: the requested
		// name is held by an entry this change can never clear — an invalid
		// index on a different table, or one the server will not drop
		// concurrently (a partitioned table's index, an index partition, a
		// constraint's index). Retrying unchanged reproduces the verdict.
		// The typed-verdict path replaces this cause and remedy with the
		// code's own advice; this mapping keeps the vocabulary total.
		return &refusal{reason: "invalid-index-occupied",
			cause:  fmt.Sprintf("an invalid index the change to %s cannot clear holds a name it needs", quotedTable(table)),
			remedy: "rename the index in the schema file and re-plan, or resolve the entry on the target"}, true
	case executor.CodePoolTooSmall:
		// The pool is sized by the target DSN, so every retry against the
		// same configuration is refused at admission the same way; only an
		// operator raising the pool ceiling changes the outcome.
		return &refusal{reason: "pool-too-small",
			cause:  fmt.Sprintf("the target's connection pool cannot hold every session the change to %s needs at once", quotedTable(table)),
			remedy: "raise the pool size on the target DSN, then re-run"}, true
	case executor.CodeBudgetLockExceeded, executor.CodeCancelledByCaller,
		executor.CodeCancelledExternally, executor.CodeInvalidIndexOwnLeftover,
		executor.CodeInvalidIndexAbandoned, executor.CodeInvalidIndexBuildInFlight,
		executor.CodeInvalidIndexBuilderUnobservable, executor.CodeInvalidIndexUnproven,
		executor.CodePartitionedIndexUnsettled, executor.CodePartitionedKeyUnsettled,
		executor.CodePartitionedForeignKeyUnsettled, executor.CodePartitionedForeignKeyLeafChanged,
		executor.CodeDetachInterrupted, executor.CodeExecutionFailed:
		// Operational outcomes: a bounded lock race, the caller's own
		// context ending or an external stop, an invalid-index state a retry
		// recovers or an operator clears or waits out, a partitioned index,
		// key, or foreign key build that a rerun resumes, a concurrent detach that running the
		// same statement again finishes, or a failure outside the typed set. A retry can succeed once conditions change, so
		// none is a permanent refusal.
		return nil, true
	}
	return nil, false
}

// createCollisionRemedy leads with a re-plan because that alone resolves a
// lost race for the table name: the next plan sees the occupant and diffs
// against it. Only a collision that survives a re-plan is a schema-file
// problem — an explicitly named constraint or index claims a name another
// relation holds (an unnamed one picks a free name on its own), or a serial
// column's auto-named sequence lands on a standalone type of that name —
// and then the operator changes the name on one side or the other.
const createCollisionRemedy = "re-plan, and if it recurs drop or rename the occupant or give the constraint, index, or sequence another name"

// indexDropInterruptedRefusal names the index whose concurrent drop was
// cancelled and the statement that finishes it. The statement is built from
// the plan's schema and index name with SQL identifier quoting, which
// PostgreSQL's identifier length bounds, so it runs as shown.
func indexDropInterruptedRefusal(err *indexDropInterruptedError, table string) *refusal {
	index := boundedQuote(err.index, quotedTableWidth)
	reason := "index-drop-cancelled"
	what := "was cancelled before it finished"
	if err.timedOut {
		reason = "index-drop-timed-out"
		what = fmt.Sprintf("ran past %s and was cancelled", err.limit)
	}
	return &refusal{reason: reason,
		cause: fmt.Sprintf("the drop of index %s on %s %s, which can leave the index invalid", index, quotedTable(table), what),
		remedy: fmt.Sprintf("if index %s is still on the table and invalid, finish the drop outside SchemaBot by running %s, then %s",
			index, err.finishDropStatement(), replanRemedy)}
}

// createCollisionRefusal names the kinds of relation that can hold a name
// the create set claims, so the operator knows where to look; the server
// error identifying the occupant stays in the logs. The list is the
// occupant's side, not the claimant's — a named constraint claims a name
// through the index it creates, so the index is what occupies it. The
// wording is kept short because the composed detail must survive the
// narrowest operator surface with its remedy's lead intact.
func createCollisionRefusal(table string) *refusal {
	return &refusal{
		reason: "create-collision",
		cause:  fmt.Sprintf("a name the create set for %s needs is already occupied (table, view, index, or sequence)", quotedTable(table)),
		remedy: createCollisionRemedy,
	}
}

// createNameMismatchRemedy is an operator's, not a retry's: the CREATE TABLE
// committed, so re-running the identical plan collides with the table this
// apply created, and the table stands under a name the schema file did not
// choose. Re-planning comes last because only the current schema, with the
// relation renamed or the table gone, tells the next plan what remains. The
// lead is the clause the cause's enumerations make room for: it is what the
// narrowest operator surface must still show once the detail is cut from
// the tail, because it alone says the table is standing and needs the name
// freed.
const (
	createNameMismatchLead   = "free the first-choice name"
	createNameMismatchRemedy = createNameMismatchLead + " and rename the owned relation to it, or drop the table, then " + replanRemedy
)

// createNameMismatchRefusal takes its cause from the caller because the
// typed error names the claimed names the table lacks and the names it owns
// instead, while the bare outcome code knows only that the two differ.
func createNameMismatchRefusal(cause string) *refusal {
	return &refusal{
		reason: "create-name-mismatch",
		cause:  cause,
		remedy: createNameMismatchRemedy,
	}
}

// createNameMismatchCause names the constraint-index or sequence names the
// committed table lacks and the suffixed names the server chose instead, so
// the operator knows which relation to rename. The names come from the
// schema file and the catalog, the same provenance as the table name every
// refusal already renders. The typed error always carries at least one
// missing name — a table that honoured every claim is not a mismatch — but
// may own nothing unclaimed when a relation was dropped inside the read-back
// window, so the owned clause is rendered only when there is one to name.
//
// The enumerations get the byte room the quoted table name and the step
// clause the detail will carry leave them; when the owned clause is rendered,
// its own prose is paid for first and the two lists share what remains.
func createNameMismatchCause(table string, mismatch *executor.CreateNameMismatchError, stepClause string) string {
	quoted := quotedTable(table)
	room := mismatchNamesRoom - len(quoted)
	if stepClause != "" {
		room -= len(clauseSeparator) + len(stepClause)
	}
	if len(mismatch.Unclaimed) == 0 {
		return fmt.Sprintf(mismatchCauseFormat, quoted, quotedNames(mismatch.Missing, room))
	}
	room -= len(mismatchOwnedFormat) - len("%s")
	missing, unclaimed := mismatchNames(mismatch.Missing, mismatch.Unclaimed, room)
	return fmt.Sprintf(mismatchCauseFormat+mismatchOwnedFormat, quoted, missing, unclaimed)
}

const (
	mismatchCauseFormat = "the CREATE TABLE for %s committed but the table does not own %s the schema file claims"
	mismatchOwnedFormat = "; it owns %s instead"
)

// mismatchNamesRoom is the byte room a mismatch cause has for the quoted
// table name, the failed-step clause, and the identifiers it enumerates
// together, before the owned clause takes its prose. The narrowest operator
// surface keeps apitypes.StatusFailureReasonKeptWidth bytes of a refusal
// detail and cuts the rest from the tail, where the remedy sits, and the
// remedy is the only clause that says the table is still standing and needs
// renaming or dropping. The room is what that surface leaves once the fixed
// prose around the names and the remedy's lead, with the separator before
// it, have taken theirs, so the enumerations shrink as the table name and
// the step clause grow and the lead survives for a table name of any legal
// length and a create set of any size.
const mismatchNamesRoom = apitypes.StatusFailureReasonKeptWidth -
	(len(mismatchCauseFormat) - 2*len("%s")) -
	len(clauseSeparator) - len(createNameMismatchLead)

// quotedTableWidth bounds the rendering of a table name in a refusal cause:
// the quoted form of an identifier of the maximum legal length whose
// characters need no escaping.
const quotedTableWidth = len(`"`) + 63 + len(`"`)

// quotedTable renders a table name for a refusal cause whose remedy is
// pinned inside the narrowest surface's clamp. The rendering is the quoted
// identifier, bounded at quotedTableWidth: quoting escapes a quote, a
// backslash, or a control character into more bytes than the identifier
// carries, and an identifier of legal length may be made of such characters,
// so a name is cut when its quoted form would take more than the room a
// plain name of the same legal length takes. The cut costs the operator
// nothing they rely on — every surface renders the detail beside the table
// it belongs to.
func quotedTable(table string) string {
	return boundedQuote(table, quotedTableWidth)
}

// quotedCutMark ends a quoted identifier that was cut to fit its width, in
// place of the closing quote; quotedCutWidth is the narrowest rendering a
// cut can leave, an opening quote and the mark, which a width below it
// still gets.
const (
	quotedCutMark  = `..."`
	quotedCutWidth = len(`"`) + len(quotedCutMark)
)

// boundedQuote renders an identifier quoted inside a byte width: the quoted
// form when it fits, otherwise the longest prefix that fits with the cut
// mark in place of its closing quote. The prefix is cut on a rune boundary,
// and by requoting rather than slicing the quoted form, so a cut never
// splits an escape sequence.
func boundedQuote(name string, width int) string {
	if q := strconv.Quote(name); len(q) <= width {
		return q
	}
	kept := `"`
	for i := range name {
		q := strconv.Quote(name[:i])
		if len(q)-len(`"`)+len(quotedCutMark) > width {
			break
		}
		kept = q[:len(q)-len(`"`)]
	}
	return kept + quotedCutMark
}

// mismatchNames renders the missing names and the owned names inside one
// byte room. A missing name and the suffixed name the server chose in its
// place are the two sides of one comparison, so the room goes to pairs first:
// the rendering shows as many names from both lists as fit together, and
// only then spends what is left on further names from one list, the missing
// names ahead of the owned ones. Pairs first is also what keeps the owned
// side — the relation the remedy says to rename, and the longer name by its
// suffix — from losing every tie to the shorter name it displaced.
//
// The search runs over the counts a rendering inside the room could show,
// not over every name the lists hold, and renders each count once, so its
// work is bounded by the room rather than by the width of the table.
func mismatchNames(missing, unclaimed []string, room int) (string, string) {
	missingRenders := leadingNameRenders(missing, room)
	unclaimedRenders := leadingNameRenders(unclaimed, room)
	bestI, bestJ := 0, 0
	for i, rendered := range missingRenders {
		for j, owned := range unclaimedRenders {
			if len(rendered)+len(owned) > room {
				continue
			}
			if outranksNamePick(i, j, bestI, bestJ) {
				bestI, bestJ = i, j
			}
		}
	}
	return missingRenders[bestI], unclaimedRenders[bestJ]
}

// leadingNameRenders renders, indexed by count, every count of leading names
// a rendering inside the room could show, from none up to namesWithinRoom.
func leadingNameRenders(names []string, room int) []string {
	renders := make([]string, 0, namesWithinRoom(names, room)+1)
	for n := range namesWithinRoom(names, room) + 1 {
		renders = append(renders, leadingNames(names, n))
	}
	return renders
}

// namesWithinRoom bounds the count of names a rendering inside the room can
// show. A quoted name costs at least its two quotes and one character, so no
// rendering that fits shows more names than the room holds such names, and
// a count past that bound is rejected without being rendered.
func namesWithinRoom(names []string, room int) int {
	return min(len(names), max(room, 0)/len(`"a"`))
}

// outranksNamePick orders two ways of showing i missing and j owned names:
// more pairs first, then more names, then more missing names.
func outranksNamePick(i, j, bestI, bestJ int) bool {
	if min(i, j) != min(bestI, bestJ) {
		return min(i, j) > min(bestI, bestJ)
	}
	if i+j != bestI+bestJ {
		return i+j > bestI+bestJ
	}
	return i > bestI
}

// quotedNames renders identifiers for a refusal cause inside a byte budget:
// it enumerates the most leading names that fit alongside a count of the
// rest, and falls back to the count alone when not even the first name fits.
// The search starts at the most names the budget could hold, not at the end
// of the list, so its work is bounded by the budget.
func quotedNames(names []string, budget int) string {
	for n := namesWithinRoom(names, budget); n > 0; n-- {
		if rendered := leadingNames(names, n); len(rendered) <= budget {
			return rendered
		}
	}
	return countedNames(len(names))
}

// leadingNames renders the first n names quoted, followed by a count of the
// rest when any remain; n of zero renders the count alone.
func leadingNames(names []string, n int) string {
	if n == 0 {
		return countedNames(len(names))
	}
	quoted := make([]string, 0, n)
	for _, name := range names[:n] {
		quoted = append(quoted, strconv.Quote(name))
	}
	rendered := strings.Join(quoted, ", ")
	if rest := len(names) - n; rest > 0 {
		rendered += remainingNames(rest)
	}
	return rendered
}

func remainingNames(rest int) string {
	return fmt.Sprintf(", and %d more", rest)
}

func countedNames(n int) string {
	if n == 1 {
		return "1 name"
	}
	return fmt.Sprintf("%d names", n)
}

// createNamesUnverifiedRefusal is decided by the wrap, not by the cause it
// carries. pg-sprite leaves the outcome retryable because the read that
// failed could be repeated on its own. SchemaBot cannot repeat only the
// read: its retry re-runs the identical plan, whose CREATE TABLE has already
// committed, so every retry collides with the table this apply created. The
// table stands with unproven names, and proving them is an operator's
// comparison, not a retry's. The cause is kept terse — the remedy, not the
// cause, names which names to compare — so that the remedy's lead survives
// the narrowest operator surface for a table name of any legal length.
func createNamesUnverifiedRefusal(table string) *refusal {
	return &refusal{
		reason: "create-names-unverified",
		cause:  fmt.Sprintf("the CREATE TABLE for %s committed but the names it owns could not be read back", quotedTable(table)),
		remedy: "compare the table's constraint-index and sequence names against the schema file, then " + replanRemedy,
	}
}

// createOwnerMismatchRefusal names both roles when the typed error carries
// them; a nil mismatch is the bare code, which knows only that the owner
// differs. Either way the table stands as committed: the executor never
// repairs ownership, so the operator inspects or drops it before a re-plan.
func createOwnerMismatchRefusal(table string, mismatch *executor.CreateOwnerMismatchError) *refusal {
	cause := fmt.Sprintf("the CREATE TABLE for %s committed under a role other than the configured owner", quotedTable(table))
	if mismatch != nil {
		cause = fmt.Sprintf("the CREATE TABLE for %s committed with owner %s instead of %s",
			quotedTable(table), strconv.Quote(mismatch.Actual), strconv.Quote(mismatch.Expected))
	}
	return &refusal{
		reason: "create-owner-mismatch",
		cause:  cause,
		remedy: "nothing was repaired; inspect or drop the table, then " + replanRemedy,
	}
}

// createOwnerUnverifiedRefusal is decided by the wrap, not by the cause it
// carries, for the same reason as createNamesUnverifiedRefusal: pg-sprite
// leaves the outcome retryable because the owner read could be repeated on
// its own, but SchemaBot's retry re-runs the identical plan, whose CREATE
// TABLE has already committed.
func createOwnerUnverifiedRefusal(table string) *refusal {
	return &refusal{
		reason: "create-owner-unverified",
		cause:  fmt.Sprintf("the CREATE TABLE for %s committed but its owner could not be read back", quotedTable(table)),
		remedy: "nothing was repaired; inspect or drop the table, then " + replanRemedy,
	}
}

func tableNotFoundRefusal(table string) *refusal {
	return &refusal{
		reason: "table-not-found",
		cause:  fmt.Sprintf("table %s does not exist on the target", quotedTable(table)),
		remedy: replanRemedy,
	}
}

// executeOptimistic runs the planned change through pg-sprite's executors,
// each feeding the tracker so a concurrent Progress poll reads the step and
// statement in flight.
func executeOptimistic(ctx context.Context, conn targetConn, change nativeApply, tableSizeLimit int64, tracker *progress.Tracker, logger *slog.Logger, tableOwner string) error {
	poolCfg, err := spritePoolConfig(conn.dsn, conn.caCertPath)
	if err != nil {
		return fmt.Errorf("prepare pg-sprite apply pool for table %q: %w", change.table, err)
	}
	pool, err := dbconn.NewPool(ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("open pg-sprite apply pool: %w", err)
	}
	defer pool.Close()

	if change.rowSecurity != nil && change.createsTable {
		_, err := executor.ExecuteReviewedCreateWithRowSecurity(ctx, pool, change.namespace, tableOwner, *change.rowSecurity, change.reviewedSecurity, executor.Budget{
			LockTimeout: optimisticLockTimeout, StatementTimeout: optimisticStatementLimit,
		})
		if err != nil {
			return fmt.Errorf("create table %q.%q with its row security: %w", change.namespace, change.table, err)
		}
		return nil
	}
	if change.rowSecurity != nil {
		_, err := executor.ExecuteReviewedRowSecurity(ctx, pool, change.namespace, *change.rowSecurity, change.reviewedSecurity, executor.Budget{
			LockTimeout: optimisticLockTimeout, StatementTimeout: optimisticStatementLimit,
		})
		if err != nil {
			return fmt.Errorf("apply row security for %q.%q: %w", change.namespace, change.table, err)
		}
		return nil
	}
	if change.enumValueAdd {
		return executeEnumValueAdd(ctx, pool, change)
	}
	if change.droppedTable != "" {
		return executeTableDrop(ctx, pool, change, tracker, logger)
	}
	statements, err := postgresCreateSetStatements(change.sql)
	if err != nil {
		return fmt.Errorf("parse planned PostgreSQL DDL for table %q: %w", change.table, err)
	}
	tier, err := preflight.RequiredTier(statements)
	if err != nil {
		return fmt.Errorf("derive privilege tier for table %q: %w", change.table, err)
	}
	if tier == preflight.TierCreateTable {
		// The off-ladder create tier has its own preflight sequence: the
		// ladder checks below state facts about an existing table, and a
		// greenfield target has none.
		return executeCreate(ctx, pool, change, statements, tracker, tableOwner)
	}
	if len(statements) != 1 {
		return fmt.Errorf("execute PostgreSQL table %q: privilege tier %s requires exactly one statement, got %d", change.table, tier, len(statements))
	}
	statement, err := pgstatement.ParseOne(statements[0])
	if err != nil {
		return fmt.Errorf("parse planned PostgreSQL statement for table %q: %w", change.table, err)
	}
	if _, err := preflight.CheckPrivileges(ctx, pool, change.namespace, change.table, preflight.Requirement{Tier: tier}); err != nil {
		return fmt.Errorf("check privileges for PostgreSQL table %q: %w", change.table, err)
	}
	// Concurrent and partitioned index builds and constraint validations are
	// bounded by their duration envelopes rather than the rewrite ceiling. NoSizeLimit still proves the
	// target exists and is an ordinary or partitioned table.
	preflightLimit := applyTablePreflightLimit(change, tableSizeLimit)
	table, err := checkTableUnlessCatalogOnly(ctx, pool, change, statement, preflightLimit, logger)
	if err != nil {
		return err
	}
	if change.partitionedIndex {
		// The build refuses a table that is no longer partitioned before it
		// uses a session, so a plan made against a partitioned parent never
		// runs here as a blocking CREATE INDEX on a plain table.
		return buildPartitionedIndex(ctx, poolCfg, pool, table, change, tracker, logger)
	}
	if table.Partitioned() {
		// The partition admission policy runs here, in the session window
		// that executes, not only at plan time: a table partitioned after
		// planning must get the typed refusal, never a raw server error
		// mid-statement. Server major matters to the policy (NOT VALID
		// foreign keys), so it is read from the same target.
		facts, err := preflight.LookupTargetFacts(ctx, pool, change.namespace, change.table)
		if err != nil {
			return fmt.Errorf("look up PostgreSQL target facts for table %q: %w", change.table, err)
		}
		if err := preflight.CheckPartitionSupport(table, facts.ServerMajor(), []string{change.sql}); err != nil {
			return fmt.Errorf("admit statement for partitioned PostgreSQL table %q: %w", change.table, err)
		}
	}
	if change.concurrentIndex {
		// A concurrent build cannot run inside a transaction block, so it
		// must not reach the transactional optimistic executor.
		return buildIndexConcurrently(ctx, pool, change, tracker, logger)
	}
	if change.indexDrop {
		// A concurrent drop cannot run inside a transaction block either, so
		// it takes pg-sprite's checked concurrent drop.
		return executeIndexDrop(ctx, pool, change, tracker, logger)
	}
	// Every other statement runs exactly as reviewed, under the
	// per-statement and lock limits. The apply never rewrites a statement
	// here: it executes the DDL the plan surfaced, which tableChanges
	// renders from the plan's ExecSQL — the submitted SQL stands in only
	// for a step carrying a blocked verdict, and a blocked step is refused
	// before an apply is queued. For an index added to an existing table,
	// the choice between a blocking and a concurrent build is made upstream
	// in pg-sprite: its planner constructs the concurrent form as the safer
	// sequence and its router promotes that into ExecSQL, so such an index
	// takes the concurrent branch above. A blocking CREATE INDEX does not
	// reach this call: a plain index on an existing table is rewritten at
	// plan time or, when the rewrite could not be constructed, blocked; an
	// index on a partitioned parent branches off above to the partitioned
	// build, which builds each partition's index concurrently; and an index
	// declared with a new table diverts to executeCreate,
	// where greenfieldCreateSet keeps the blocking form on purpose — a
	// table born in the run has no readers, and CONCURRENTLY cannot run
	// inside its create sequence.
	if change.dropped != nil {
		return executeReviewedDrop(ctx, pool, table, change, statement.SQL(), tracker)
	}
	if change.validates != nil {
		// pg-sprite reads the constraint again under the validation's lock
		// and refuses with ErrValidatedConstraintDrift, validating nothing,
		// when it no longer has the definition the plan recorded.
		// The scan runs under the validation bound; its lock wait keeps a
		// short lock budget, so queueing behind other DDL never holds the
		// apply for the whole bound.
		if change.validateMaxDuration <= 0 {
			return fmt.Errorf("validate reviewed constraint %q on PostgreSQL table %q: validation bound is not set", change.validates.Constraint, change.table)
		}
		if err := executor.ExecuteNativeValidatingWithProgress(ctx, pool, table, statement, executor.Budget{
			LockTimeout: validationLockBudget(change.validateMaxDuration), StatementTimeout: change.validateMaxDuration,
		}, executor.DefaultRetryPolicy(), *change.validates, tracker); err != nil {
			return fmt.Errorf("validate reviewed constraint %q on PostgreSQL table %q: %w", change.validates.Constraint, change.table,
				nameValidationBound(err, change.validates.Constraint, change.validateMaxDuration))
		}
		return nil
	}
	if change.adopts != nil {
		// pg-sprite reads the index again under the table's lock and
		// refuses with ErrAdoptedIndexDrift, attaching nothing, when it no
		// longer has the definition the plan matched.
		if err := executor.ExecuteNativeAdoptingWithProgress(ctx, pool, table, statement, executor.Budget{
			LockTimeout: optimisticLockTimeout, StatementTimeout: optimisticStatementLimit,
		}, executor.DefaultRetryPolicy(), *change.adopts, tracker); err != nil {
			return fmt.Errorf("attach reviewed index %q on PostgreSQL table %q: %w", change.adopts.Index, change.table, err)
		}
		return nil
	}
	if statement.Kind() == pgstatement.KindRenameIndex {
		// ALTER INDEX ... RENAME TO names the index, not its table, so it
		// takes pg-sprite's index rename: under the table's lock it checks
		// that the name still resolves to a plain index of this table, and
		// after the rename it confirms the renamed relation is that index.
		if err := executor.ExecuteIndexRenameWithProgress(ctx, pool, table, statement, executor.Budget{
			LockTimeout: optimisticLockTimeout, StatementTimeout: optimisticStatementLimit,
		}, executor.DefaultRetryPolicy(), tracker); err != nil {
			return fmt.Errorf("rename index on PostgreSQL table %q: %w", change.table, err)
		}
		return nil
	}
	if err := executor.ExecuteNativeWithProgress(ctx, pool, table, statement, executor.Budget{
		LockTimeout: optimisticLockTimeout, StatementTimeout: optimisticStatementLimit,
	}, executor.DefaultRetryPolicy(), tracker); err != nil {
		return fmt.Errorf("execute native-safe PostgreSQL statement on table %q: %w", change.table, err)
	}
	return nil
}

// executeReviewedDrop runs a statement that drops a live object through
// pg-sprite's drop check: under the table's lock, in the transaction that
// drops it, the object must still have the definition the plan recorded, and
// when it does not the executor refuses with ErrDroppedObjectDrift and drops
// nothing. The statement runs under the same lock and statement limits as
// every other optimistic statement; the concurrent and validation budgets
// bound step classes a single drop never contains, and are set only because
// the executor refuses an unbounded budget set.
func executeReviewedDrop(ctx context.Context, pool *pgxpool.Pool, table preflight.PreflightedTable, change nativeApply, statement string, tracker *progress.Tracker) (err error) {
	if tracker == nil {
		return fmt.Errorf("drop reviewed %s %q on PostgreSQL table %q: progress tracker is required", change.dropped.Kind, change.dropped.Name, change.table)
	}
	tracker.Start(1, progress.OperationOptimistic)
	defer func() { tracker.Finish(err) }()
	at := executor.RunStep{Tracker: tracker, Step: 1}
	budget := executor.SequenceBudget{
		Brief:      executor.Budget{LockTimeout: optimisticLockTimeout, StatementTimeout: optimisticStatementLimit},
		Concurrent: executor.ConcurrentBudget{Overall: optimisticStatementLimit},
		Validate:   executor.ValidateBudget{LockTimeout: optimisticLockTimeout, Overall: optimisticStatementLimit},
	}
	if _, err := executor.RunSequenceAsStep(ctx, pool, table, []string{statement}, budget, executor.DefaultRetryPolicy(), change.adopts, nil, change.dropped, at); err != nil {
		return fmt.Errorf("drop reviewed %s %q on PostgreSQL table %q: %w", change.dropped.Kind, change.dropped.Name, change.table, err)
	}
	return nil
}

// checkTableUnlessCatalogOnly runs the table preflight under limit. When the
// table is over the limit, the statement still runs if pg-sprite proves,
// against the live table now, that every operation in it changes only the
// catalog: such a statement reads and writes no rows, so its cost under the
// lock does not grow with the table. The proof is made here rather than
// read from the plan, because the table, its column types, and the
// functions a default calls can all change between plan and apply. A proof
// that cannot be made leaves the size refusal standing.
func checkTableUnlessCatalogOnly(ctx context.Context, pool *pgxpool.Pool, change nativeApply, statement pgstatement.Statement, limit int64, logger *slog.Logger) (preflight.PreflightedTable, error) {
	table, err := preflight.CheckTable(ctx, pool, change.namespace, change.table, limit)
	var sizeErr *preflight.SizeError
	if !errors.As(err, &sizeErr) {
		if err != nil {
			return preflight.PreflightedTable{}, fmt.Errorf("preflight PostgreSQL table %q: %w", change.table, err)
		}
		return table, nil
	}
	catalogOnly, proofErr := provenCatalogOnly(ctx, pool, statement)
	if proofErr != nil {
		logger.Warn("PostgreSQL table is over the size limit and the statement could not be proven to change only the catalog; the apply will be refused on size",
			"namespace", change.namespace,
			"table", change.table,
			"size_limit_bytes", limit,
			"error", proofErr)
		return preflight.PreflightedTable{}, fmt.Errorf("preflight PostgreSQL table %q: %w", change.table, &catalogOnlyProofError{size: sizeErr, proof: proofErr})
	}
	if !catalogOnly {
		return preflight.PreflightedTable{}, fmt.Errorf("preflight PostgreSQL table %q: %w", change.table, err)
	}
	logger.Info("PostgreSQL table is over the size limit; running the statement because it changes only the catalog",
		"namespace", change.namespace,
		"table", change.table,
		"size_limit_bytes", limit)
	table, err = preflight.CheckTable(ctx, pool, change.namespace, change.table, preflight.NoSizeLimit)
	if err != nil {
		return preflight.PreflightedTable{}, fmt.Errorf("preflight PostgreSQL table %q: %w", change.table, err)
	}
	return table, nil
}

// sqlstateInsufficientPrivilege is the SQLSTATE the server returns when a
// role lacks a privilege the statement needs.
const sqlstateInsufficientPrivilege = "42501"

// sqlstateUndefinedTable is undefined_table: a statement names a relation
// the database does not have.
const sqlstateUndefinedTable = "42P01"

// catalogOnlyProofError reports a table over the size limit whose statement
// went unproven because the proof itself failed, not because the statement
// rewrites or scans rows. It is not a size verdict: a retry, or a grant the
// proof needs, can let the statement run. So it unwraps to the proof's
// failure and never to the size error, and the size refusal cannot claim it.
type catalogOnlyProofError struct {
	size  *preflight.SizeError
	proof error
}

func (e *catalogOnlyProofError) Error() string {
	return fmt.Sprintf("%s; proving the statement changes only the catalog failed: %s", e.size, e.proof)
}

func (e *catalogOnlyProofError) Unwrap() error { return e.proof }

// catalogOnlyProofRefusal decides the outcome of a failed catalog-only
// proof. A role that cannot create the proof's temporary table needs a
// grant, so that refusal names it. Any other failure, such as a dropped
// connection or a server that briefly accepts no writes during a failover,
// is operational, and a retry may prove the statement.
func catalogOnlyProofRefusal(err *catalogOnlyProofError, table string) *refusal {
	if probeLacksTemporaryGrant(err.proof) {
		return &refusal{reason: "insufficient-privileges",
			cause:         fmt.Sprintf("table %s is over the size limit, and proving the statement changes only the catalog needs a temporary table the engine role cannot create", quotedTable(table)),
			remedy:        "grant TEMPORARY on the database to the engine role, then retry the apply",
			provisionable: true}
	}
	return nil
}

// probeLacksTemporaryGrant reports whether the add-column proof could not
// create its temporary table because the engine role lacks the privilege.
func probeLacksTemporaryGrant(err error) bool {
	if !errors.Is(err, preflight.ErrAddColumnProbeUnavailable) {
		return false
	}
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == sqlstateInsufficientPrivilege
}

// provenCatalogOnly classifies statement against the live table and reports
// whether pg-sprite proved every operation in it changes only the catalog.
func provenCatalogOnly(ctx context.Context, pool *pgxpool.Pool, statement pgstatement.Statement) (bool, error) {
	facts, err := migrate.LiveFacts(ctx, pool, statement)
	if err != nil {
		return false, fmt.Errorf("read live facts: %w", err)
	}
	plan, err := planner.Classify(statement.SQL(), facts.Classifier)
	if err != nil {
		return false, fmt.Errorf("classify the statement: %w", err)
	}
	return planner.ProvenCatalogOnly(plan.Decisions), nil
}

func applyTablePreflightLimit(change nativeApply, tableSizeLimit int64) int64 {
	if change.durationBoundedIndexBuild() || change.durationBoundedValidation() {
		return preflight.NoSizeLimit
	}
	if change.indexDrop {
		// A concurrent index drop scans no rows, so the table's size does
		// not bound it.
		return preflight.NoSizeLimit
	}
	return tableSizeLimit
}

// executeCreate runs a greenfield create set through pg-sprite's create path:
// parse the table and its declared indexes, prove the role can create in the schema,
// prove the name is free, then execute — both proofs minted here, in the
// session that executes, because absence or privilege at plan time proves
// nothing about apply time. The table size gate deliberately does not run:
// it bounds rewrites of existing data, and a table that does not exist yet
// has none.
func executeCreate(ctx context.Context, pool *pgxpool.Pool, change nativeApply, statements []string, tracker *progress.Tracker, tableOwner string) error {
	// The planned statements arrive schema-qualified; the desired-schema
	// contract wants the unqualified form and the executor pins the schema
	// from the absence proof instead.
	unqualified := make([]string, len(statements))
	for i, statement := range statements {
		var err error
		unqualified[i], err = pgstatement.Qualify(statement, "")
		if err != nil {
			return fmt.Errorf("render planned create statement %d for table %q in unqualified form: %w", i+1, change.table, err)
		}
	}
	desired, err := pgstatement.ParseDesired(strings.Join(unqualified, ";\n"))
	if err != nil {
		return fmt.Errorf("parse planned CREATE TABLE for table %q: %w", change.table, err)
	}
	role, err := preflight.CheckCreatePrivilegesAs(ctx, pool, change.namespace, tableOwner)
	if err != nil {
		return fmt.Errorf("check creation access in PostgreSQL schema %q: %w", change.namespace, err)
	}
	absent, err := preflight.CheckTableAbsent(ctx, pool, change.namespace, change.table)
	if err != nil {
		return fmt.Errorf("verify PostgreSQL table %q is absent: %w", change.table, err)
	}
	if _, err := executor.ExecuteCreateWithProgress(ctx, pool, absent, role, desired, executor.Budget{
		LockTimeout: optimisticLockTimeout, StatementTimeout: optimisticStatementLimit,
	}, executor.DefaultRetryPolicy(), tracker); err != nil {
		return fmt.Errorf("execute PostgreSQL CREATE TABLE %q: %w", change.table, err)
	}
	return nil
}

// concurrentIndexExecutor names the executor entry points a concurrent
// index build reaches: the build itself and the recovery run over an
// abandoned invalid index. It is a test seam, so a test can observe the
// budget and the context deadline the drive hands each one against an
// executor it scripts instead of a target to dial; the package default is
// the executor's own functions. The engine's execute seam cannot reach these
// calls, since it replaces the whole of executeOptimistic. The seam is
// package state shared by every drive in the process, so swapping it is not
// safe from a test that runs in parallel with another; scriptConcurrentIndex
// restores it when each test ends.
type concurrentIndexExecutor struct {
	build   func(ctx context.Context, pool *pgxpool.Pool, sql string, budget executor.ConcurrentBudget, tracker *progress.Tracker) (executor.IndexBuildReport, error)
	rebuild func(ctx context.Context, pool *pgxpool.Pool, sql string, budget executor.ConcurrentBudget) (executor.IndexRecoveryReport, error)
	drop    func(ctx context.Context, pool *pgxpool.Pool, sql string, budget executor.ConcurrentBudget) (executor.IndexRecoveryReport, error)
}

var concurrentIndex = concurrentIndexExecutor{
	build:   executor.BuildIndexConcurrentlyWithProgress,
	rebuild: executor.RebuildAbandonedIndex,
	drop:    executor.DropAbandonedIndex,
}

// partitionedIndexBuild is the executor entry point a partitioned index build
// reaches. It is a test seam, so a test can observe the budget, the deadline
// and the table the drive hands the build against an executor it scripts; the
// package default is the executor's own function. Like concurrentIndex, it is
// package state shared by every drive in the process, so swapping it is not
// safe from a test that runs in parallel with another.
var partitionedIndexBuild = executor.BuildPartitionedIndexWithProgress

// partitionedIndexCancelledDetail is the operator-facing outcome of a
// partitioned index build an operator cancelled.
const partitionedIndexCancelledDetail = "Partitioned index build cancelled; the partitions already built keep their index, and applying the plan again resumes the build"

// buildPartitionedIndex adds an index to a partitioned table through
// pg-sprite's partitioned index build: it creates the parent's index with ON
// ONLY, builds each partition's index concurrently and attaches it, and
// returns once the parent's index is verified valid. The statement never runs
// as written, which would block writes to the whole table while every
// partition's index is built. The executor refuses a table that is not
// partitioned before it touches the target, so a table that stopped being
// partitioned after planning is a typed refusal rather than a blocking build.
//
// Each partition's build gets the concurrent index bound as its budget, and
// the whole build gets the same bound as its deadline, so one apply is bounded
// the way a concurrent build on a plain table is. A build that runs past the
// deadline keeps every partition it finished, and the retry resumes from
// them. An operator cancel signals the partition build in flight through the
// tracker; the drive classifies the outcome.
func buildPartitionedIndex(ctx context.Context, cfg dbconn.Config, pool *pgxpool.Pool, table preflight.PreflightedTable, change nativeApply, tracker *progress.Tracker, logger *slog.Logger) error {
	if change.concurrentIndexMaxDuration <= 0 {
		// The same guard as buildIndexConcurrently: the drive stamps the
		// bound on every change, so only a nativeApply built without the
		// drive reaches this branch.
		return fmt.Errorf("build PostgreSQL index on partitioned table %q: build bound is unset", change.table)
	}
	buildCtx, cancel := context.WithTimeout(ctx, change.concurrentIndexMaxDuration)
	defer cancel()
	start := time.Now()
	report, err := partitionedIndexBuild(buildCtx, cfg, pool, table, change.sql,
		executor.ConcurrentBudget{Overall: change.concurrentIndexMaxDuration}, tracker)
	if err == nil {
		logger.Info("PostgreSQL partitioned index build finished",
			"namespace", change.namespace, "table", change.table, "index", report.Index,
			"partitions", report.Partitions, "partitions_built", len(report.Worked),
			"parent_created", report.ParentCreated, "duration", report.Duration)
		return nil
	}
	if partitionedBuildRanPastBound(ctx, buildCtx) {
		// The deadline is the outcome, whatever the executor was doing when
		// it fired, so the bound error does not carry the executor's error
		// for the classifiers to read; the log does.
		built := len(report.Worked)
		if built == 0 {
			logger.Warn("PostgreSQL partitioned index build ran past its bound before it finished any partition; the apply is refused until the bound is raised",
				"namespace", change.namespace, "table", change.table, "bound", change.concurrentIndexMaxDuration, "error", err)
		} else {
			logger.Warn("PostgreSQL partitioned index build ran past its bound; the retry resumes from the partitions already built",
				"namespace", change.namespace, "table", change.table, "bound", change.concurrentIndexMaxDuration,
				"partitions_built", built, "error", err)
		}
		return fmt.Errorf("build PostgreSQL index on partitioned table %q: %w", change.table,
			&partitionedIndexBoundError{bound: change.concurrentIndexMaxDuration, partitionsBuilt: built})
	}
	return fmt.Errorf("build PostgreSQL index on partitioned table %q: %w", change.table,
		nameConcurrentIndexBound(err, change.concurrentIndexMaxDuration, time.Since(start)))
}

// partitionedBuildRanPastBound reports whether the build's own deadline ended
// it, as opposed to the drive's context: an operator cancel or an engine halt
// ends the drive's context, and is classified as what it is.
func partitionedBuildRanPastBound(driveCtx, buildCtx context.Context) bool {
	return driveCtx.Err() == nil && errors.Is(buildCtx.Err(), context.DeadlineExceeded)
}

// partitionedIndexBoundError reports that a partitioned index build ran past
// the concurrent index bound before every partition had its index.
// partitionsBuilt is how many partitions this run gave an index, which
// decides whether a retry can make progress.
type partitionedIndexBoundError struct {
	bound           time.Duration
	partitionsBuilt int
}

func (e *partitionedIndexBoundError) Error() string {
	if e.partitionsBuilt == 0 {
		return fmt.Sprintf("the partitioned index build ran past %s (%s) before it finished any partition", concurrentIndexBoundOption, e.bound)
	}
	return fmt.Sprintf("the partitioned index build ran past %s (%s) before every partition had its index", concurrentIndexBoundOption, e.bound)
}

// buildIndexConcurrently runs a CREATE INDEX CONCURRENTLY through pg-sprite's
// dedicated index-build executor, which runs it outside a transaction block
// under the CONCURRENTLY budget policy and returns a catalog-verified
// verdict. When the executor refuses before building because an invalid
// index occupies the requested name, or sits quarantined on the table, that
// pg-sprite's recovery can prove abandoned — an earlier build's leftover with
// no backend behind it, debris an interrupted recovery renamed, or an entry
// whose builder this role cannot see through the progress view — the build
// is re-run through pg-sprite's recovery, which removes the entry under a
// lock-held proof of abandonment and then builds. That is the state a
// re-driven apply meets after a crash or cancellation mid-build, and it
// converges without an operator; every verdict the recovery cannot prove —
// a build in flight, another table's entry, an index the server will not
// drop concurrently, an unverifiable catalog — is returned as the executor
// typed it.
//
// A leftover this drive's own build produced is removed here only when the
// engine recorded that its operator cancel acted on this apply. That drop gets
// a detached bounded envelope and never rebuilds the cancelled index. Every
// other own leftover stays operational and retryable, and the next drive meets
// it as abandoned debris and recovers it before rebuilding.
//
// A recovery that fails without a fresh invalid-index verdict — its proof
// lock lost to a build still holding the table, a pool with no room for its
// extra session — is returned as an indexRecoveryError carrying the verdict
// it acted on, so the operator detail still names the index the retry acts
// on.
func buildIndexConcurrently(ctx context.Context, pool *pgxpool.Pool, change nativeApply, tracker *progress.Tracker, logger *slog.Logger) error {
	if change.concurrentIndexMaxDuration <= 0 {
		// The bound is the build's server-side statement timeout and the
		// recovery's only deadline; a non-positive one would be refused by
		// the executor as unbounded, or end every recovery before it starts
		// and report it as a cancellation, so an unset bound is refused
		// before any session is acquired. The drive cannot reach this
		// branch — the engine constructor normalizes its option and
		// runOptimisticApply stamps the result on every change — so it
		// guards a nativeApply built without going through the drive.
		return fmt.Errorf("build PostgreSQL index concurrently on table %q: build bound is unset", change.table)
	}
	// The build runs under a server-side statement_timeout of the configured
	// bound, with the apply ceiling — the bound plus the setup headroom — as
	// the client-side backstop above it. The server's timer is what ends an
	// over-long build, so exhaustion arrives as the executor's typed budget
	// verdict rather than an ambiguous cancellation, and a cancel request
	// that never reaches the server still cannot leave the statement running
	// unbounded on the target.
	start := time.Now()
	_, err := concurrentIndex.build(ctx, pool, change.sql,
		executor.ConcurrentBudget{Overall: change.concurrentIndexMaxDuration}, tracker)
	if err == nil {
		return nil
	}
	if cleanupErr := cleanupCancelledConcurrentIndex(ctx, pool, change, err, logger); cleanupErr != nil {
		return cleanupErr
	}
	var invalidErr *executor.InvalidIndexError
	if !errors.As(err, &invalidErr) || !abandonedBeforeBuild(invalidErr) {
		return fmt.Errorf("build PostgreSQL index concurrently on table %q: %w", change.table,
			nameConcurrentIndexBound(err, change.concurrentIndexMaxDuration, time.Since(start)))
	}
	logger.Info("PostgreSQL concurrent index build found an abandoned invalid index under the requested name or quarantined on the table; recovering it before the build",
		"namespace", change.namespace, "table", change.table,
		"index_schema", invalidErr.Schema, "index", invalidErr.Index, "verdict", invalidErr.Code())

	// The recovery is one envelope — abandonment proof, quarantine drops,
	// then the build — bounded as a whole by one deadline of the configured
	// bound, so the ceiling headroom pinned for the build holds for the
	// recovery too. The executor's served mode would spend that bound once
	// on the drops and once more on the build, which no ceiling built on a
	// single bound can hold; caller-owned mode makes this deadline the only
	// bound on both, and the build's own catalog verdict still runs after it
	// on the executor's detached context. Caller-owned mode runs the drops
	// and the build with no server-side statement timeout, so a cancellation
	// the server never receives leaves the statement running on the target
	// until it finishes on its own, bounded only by the lock timeouts the
	// recovery sets for itself. The refused build charged only catalog reads
	// against the ceiling, so the recovery starts with the full bound ahead
	// of it; that rests on abandonedBeforeBuild admitting only verdicts with
	// no build attached, since a verdict reached after a build ran would
	// arrive here with the bound already spent once.
	recoveryCtx, cancel := context.WithTimeout(ctx, change.concurrentIndexMaxDuration)
	defer cancel()
	report, err := concurrentIndex.rebuild(recoveryCtx, pool, change.sql,
		executor.ConcurrentBudget{CallerOwned: true})
	if err != nil {
		if cleanupErr := cleanupCancelledConcurrentIndex(ctx, pool, change, err, logger); cleanupErr != nil {
			return cleanupErr
		}
		return fmt.Errorf("rebuild PostgreSQL index concurrently on table %q: %w", change.table,
			&indexRecoveryError{verdict: invalidErr, err: err})
	}
	logger.Info("PostgreSQL concurrent index build recovered an abandoned invalid index and built the index",
		"namespace", change.namespace, "table", change.table,
		"index_schema", invalidErr.Schema, "index", invalidErr.Index,
		"dropped", len(report.Dropped), "skipped", len(report.Skipped), "duration", report.Duration)
	return nil
}

func cleanupCancelledConcurrentIndex(ctx context.Context, pool *pgxpool.Pool, change nativeApply, buildErr error, logger *slog.Logger) error {
	var invalidErr *executor.InvalidIndexError
	if !errors.As(buildErr, &invalidErr) || !cancelledBuildLeftOwnIndex(change, buildErr, invalidErr) {
		return nil
	}
	// Cancellation ends the build context, but removing the invalid catalog
	// entry is part of settling the operator's request. Detach that cleanup
	// from the cancelled apply and bound it on its own: dropping debris must
	// not consume another full build bound or hold the terminal outcome
	// open past the cancel's settle wait.
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cancelledIndexCleanupBound)
	defer cancel()
	report, cleanupErr := concurrentIndex.drop(cleanupCtx, pool, change.sql,
		executor.ConcurrentBudget{CallerOwned: true})
	if cleanupErr != nil {
		existing := invalidErr
		var cleanupVerdict *executor.InvalidIndexError
		if errors.As(cleanupErr, &cleanupVerdict) {
			existing = cleanupVerdict
		}
		logger.Error("PostgreSQL operator cancel could not remove the invalid index left by the concurrent build",
			"namespace", change.namespace, "table", change.table,
			"index_schema", existing.Schema, "index", existing.Index, "error", cleanupErr)
		return &cancelledIndexCleanupError{verdict: invalidErr, existing: existing, identityUncertain: existing == invalidErr}
	}
	// A clean return is not proof of a removal: the sweep steps over an
	// entry the server refuses to drop concurrently and reports it skipped,
	// and an entry already gone drops nothing. Only the report says what
	// happened, and the summary claims exactly that.
	outcome := &cancelledIndexCleanupError{verdict: invalidErr, cleaned: true, dropped: report.Dropped, skipped: report.Skipped}
	switch {
	case len(report.Dropped) > 0:
		logger.Info("PostgreSQL operator cancel removed the invalid index left by the concurrent build",
			"namespace", change.namespace, "table", change.table,
			"index_schema", invalidErr.Schema, "index", invalidErr.Index,
			"dropped", len(report.Dropped), "skipped", len(report.Skipped), "duration", report.Duration)
	case len(report.Skipped) > 0:
		logger.Warn("PostgreSQL operator cancel could not drop the invalid index left by the concurrent build concurrently; it stays quarantined for an operator to remove",
			"namespace", change.namespace, "table", change.table,
			"index_schema", invalidErr.Schema, "index", invalidErr.Index,
			"skipped", len(report.Skipped), "duration", report.Duration)
	default:
		logger.Info("PostgreSQL operator cancel found the invalid index left by the concurrent build already gone",
			"namespace", change.namespace, "table", change.table,
			"index_schema", invalidErr.Schema, "index", invalidErr.Index, "duration", report.Duration)
	}
	return outcome
}

func cancelledBuildLeftOwnIndex(change nativeApply, err error, invalidErr *executor.InvalidIndexError) bool {
	return isCancellation(err) && cancelledBuildMayHaveLeftIndex(invalidErr.Code()) && invalidErr.Table == change.table &&
		change.cancelRequested != nil && change.cancelRequested()
}

// cancelledBuildMayHaveLeftIndex identifies verdicts that can describe the
// invalid entry created by the cancelled build. When activity is hidden, the
// cleanup's table lock proves abandonment and refuses to act while a hidden
// builder is still alive.
func cancelledBuildMayHaveLeftIndex(code executor.Code) bool {
	switch code {
	case executor.CodeInvalidIndexOwnLeftover, executor.CodeInvalidIndexBuilderUnobservable:
		return true
	default:
		return false
	}
}

// cancelledIndexCleanupError keeps the executor's cancellation and invalid
// index verdict reachable while recording what the bounded cleanup did with
// the entry: when the cleanup ran to completion, dropped and skipped are the
// executor's own account of it, and the summary claims no more than they
// prove. Cleanup failure is deliberately not its cause: cancellation stays
// the apply outcome, while the server log carries the cleanup error.
type cancelledIndexCleanupError struct {
	verdict           *executor.InvalidIndexError
	existing          *executor.InvalidIndexError
	identityUncertain bool
	cleaned           bool
	dropped           []executor.DroppedIndex
	skipped           []executor.QuarantinedIndex
}

func (e *cancelledIndexCleanupError) Error() string { return e.verdict.Error() }
func (e *cancelledIndexCleanupError) Unwrap() error { return e.verdict }

func cancelledIndexCleanupDetail(err error) string {
	var cleanup *cancelledIndexCleanupError
	if !errors.As(err, &cleanup) {
		return ""
	}
	name := fmt.Sprintf("%q.%q", cleanup.verdict.Schema, cleanup.verdict.Index)
	if cleanup.cleaned {
		return sanitizeReasonText(cleanedIndexDetail(name, cleanup.dropped, cleanup.skipped))
	}
	if cleanup.existing != nil && !cleanup.identityUncertain {
		name = fmt.Sprintf("%q.%q", cleanup.existing.Schema, cleanup.existing.Index)
		return sanitizeReasonText(fmt.Sprintf("Concurrent index build cancelled; invalid index %s remains until an operator removes it; see the PostgreSQL invalid-index recovery guidance", name))
	}
	return sanitizeReasonText(fmt.Sprintf("Concurrent index build cancelled; the invalid index remains under %s or an identity-derived pgsprite_abandoned_<oid> name; query pg_index joined to pg_class for invalid indexes on the table, then follow the PostgreSQL invalid-index recovery guidance", name))
}

// cleanedIndexDetail words a completed cleanup from the executor's report: a
// removal only when an entry was dropped; the quarantine names when the
// server refused to drop the entry concurrently and the sweep stepped over
// it, since that is where an operator finds it; and an already-gone entry
// when the cleanup found nothing to remove.
func cleanedIndexDetail(name string, dropped []executor.DroppedIndex, skipped []executor.QuarantinedIndex) string {
	switch {
	case len(dropped) > 0:
		return fmt.Sprintf("Concurrent index build cancelled; invalid index %s was removed", name)
	case len(skipped) > 0:
		quarantined := make([]string, 0, len(skipped))
		for _, entry := range skipped {
			quarantined = append(quarantined, fmt.Sprintf("%q.%q", entry.Schema, entry.Index))
		}
		return fmt.Sprintf("Concurrent index build cancelled; invalid index %s remains quarantined as %s because the server refuses to drop it concurrently; an operator must remove it, see the PostgreSQL invalid-index recovery guidance",
			name, strings.Join(quarantined, ", "))
	default:
		return fmt.Sprintf("Concurrent index build cancelled; invalid index %s was already gone when the cleanup ran", name)
	}
}

// abandonedBeforeBuild reports whether an invalid-index verdict is one the
// build refused on before running, over an entry this actor may have
// pg-sprite's recovery remove: no build failure is wrapped, and the verdict
// is one of the two the recovery settles on this drive's behalf — an entry
// proven abandoned on the target table, or one there whose builder this
// role cannot observe, which the recovery decides under the table's lock
// rather than through the progress view, so a hidden build still holding
// the table stops it at the lock budget instead of losing its index. The
// verdicts are named rather than taken from pg-sprite's Recoverable set:
// this drive's own leftover is recoverable upstream but stays with the
// failure that produced it, and the permanent verdicts are never this
// actor's to clear.
func abandonedBeforeBuild(invalidErr *executor.InvalidIndexError) bool {
	if invalidErr.Build != nil {
		return false
	}
	switch invalidErr.Code() {
	case executor.CodeInvalidIndexAbandoned, executor.CodeInvalidIndexBuilderUnobservable:
		return true
	default:
		return false
	}
}

// indexRecoveryError reports that the recovery run over an abandoned invalid
// index failed before the index was built, keeping the verdict the recovery
// acted on beside the failure. pg-sprite types a recovery that lost its
// proof lock or found the pool too small as a budget or pool outcome with no
// index in it, yet the index under the name is still the state the retry
// acts on, so the operator detail needs the name from here. The failure
// stays reachable to errors.As, so a recovery that ends in a fresh
// invalid-index verdict is classified by that verdict, not by this wrapper.
type indexRecoveryError struct {
	verdict *executor.InvalidIndexError
	err     error
}

func (e *indexRecoveryError) Error() string {
	return fmt.Sprintf("recover abandoned invalid index %q.%q before the build: %v", e.verdict.Schema, e.verdict.Index, e.err)
}

func (e *indexRecoveryError) Unwrap() error { return e.err }

// tableSizeLimitOption is the server option that sets the largest table a
// native-safe apply changes.
const tableSizeLimitOption = "postgres.native_safe_table_size_limit_bytes"

// tableTooLargeRefusal says how big the table is against the limit, in units
// a person reads, and names the option that sets it. It says whose limit this
// is because, read cold, a size refusal sounds like a property of PostgreSQL
// or of the change, when it is SchemaBot's own conservatism for the
// native-safe path. Two sizes that round to the same figure are given in
// bytes, so the table never reads as no larger than its limit.
func tableTooLargeRefusal(sizeErr *preflight.SizeError) *refusal {
	size, limit := ui.FormatBytesBinary(sizeErr.TotalBytes), ui.FormatBytesBinary(sizeErr.LimitBytes)
	if size == limit {
		size, limit = fmt.Sprintf("%d bytes", sizeErr.TotalBytes), fmt.Sprintf("%d bytes", sizeErr.LimitBytes)
	}
	return &refusal{reason: "table-too-large",
		cause:  fmt.Sprintf("the table is %s with its indexes, over the %s limit SchemaBot sets for a native-safe apply, not a PostgreSQL limit", size, limit),
		remedy: fmt.Sprintf("raise %s to change a table this size, then plan again", tableSizeLimitOption)}
}

// concurrentIndexBoundOption is the configuration key an operator changes
// when a concurrent index build needs more time than the bound allows.
const concurrentIndexBoundOption = "postgres.concurrent_index_max_duration"

// concurrentIndexBoundError reports that a concurrent index build ran past
// the configured bound. pg-sprite types the exhaustion as a statement budget
// verdict, whose text reads like the per-statement budget an ordinary
// native-safe statement runs under; this wrapper names the option the
// operator changes instead. The verdict stays reachable to errors.As, so an
// invalid-index verdict wrapping the same exhaustion is still decided by its
// own code.
type concurrentIndexBoundError struct {
	bound time.Duration
	err   error
}

func (e *concurrentIndexBoundError) Error() string {
	return fmt.Sprintf("the concurrent index build ran past %s (%s) and was cancelled", concurrentIndexBoundOption, e.bound)
}

func (e *concurrentIndexBoundError) Unwrap() error { return e.err }

// validationBoundOption is the configuration key an operator changes when a
// constraint validation needs more time than the bound allows.
const validationBoundOption = "postgres.validate_max_duration"

// validationBoundError reports that a VALIDATE CONSTRAINT scan ran past the
// configured bound. Like concurrentIndexBoundError, it names the option the
// operator changes instead of the per-statement budget pg-sprite's verdict
// describes, and it says what the cancelled validation left live: the
// constraint, still NOT VALID, which checks new writes and not existing rows.
type validationBoundError struct {
	constraint string
	bound      time.Duration
	err        error
}

// Error is the refusal's cause. The constraint name is held to the width of
// a maximal identifier, so with the widest bound the cause and the remedy
// that follows it fit inside the status listing's kept width and the listing
// still says what to do.
func (e *validationBoundError) Error() string {
	return fmt.Sprintf("constraint %s ran past %s (%s); it stays NOT VALID and checks new writes only",
		boundedQuote(e.constraint, quotedTableWidth), validationBoundOption, e.bound)
}

func (e *validationBoundError) Unwrap() error { return e.err }

// validationLockBudget is the lock wait a validation under bound gets: the
// short lock budget, or half the bound when the bound is shorter. The lock is
// the first thing the statement waits for, so a wait behind other DDL always
// ends at its lock budget before the bound's statement timer fires, and lock
// contention is retried as contention instead of refused as an over-long scan.
func validationLockBudget(bound time.Duration) time.Duration {
	return min(optimisticLockTimeout, bound/2)
}

// validationCeiling is how long a validation apply may run in all: the
// scan's bound, the setup headroom a concurrent index build also gets, and
// the lock retries that can come before the scan starts. Each attempt but the
// last can wait out its lock budget and then back off for at most the
// policy's longest backoff, and only the attempt that gets the lock scans.
func validationCeiling(bound time.Duration) time.Duration {
	policy := executor.DefaultRetryPolicy()
	lockRetries := time.Duration(policy.MaxAttempts-1) * (validationLockBudget(bound) + policy.MaxBackoff)
	return bound + concurrentIndexHeadroom + lockRetries
}

// nameValidationBound wraps a validation failure caused by the bound's
// statement timeout firing so the operator detail names the option; every
// other failure, the lock budget among them, passes through unchanged.
func nameValidationBound(err error, constraint string, bound time.Duration) error {
	var budgetErr *executor.BudgetError
	if errors.As(err, &budgetErr) && budgetErr.Cause == executor.CauseStatement {
		return &validationBoundError{constraint: constraint, bound: bound, err: err}
	}
	return err
}

// sqlstateQueryCanceled is the SQLSTATE the server raises when a statement
// is cancelled, whether by its statement_timeout or by a cancel request.
const sqlstateQueryCanceled = "57014"

// nameConcurrentIndexBound wraps a build failure caused by the bound's
// statement timeout firing so the operator detail names the option; every
// other failure passes through unchanged. A lock budget cannot be the cause
// here — the build runs with lock_timeout disabled — so only the statement
// cause is the bound's.
//
// The bound is the build session's statement_timeout, so its timer ends
// whichever statement the session is running when it fires. For the build
// itself the executor types that as its statement budget verdict. For the
// catalog reads the executor runs in the same session ahead of the build —
// resolving the target, inspecting the requested name for an invalid entry,
// listing quarantined debris — the cancellation comes back as the server's
// raw query_canceled, and it is the bound's only when the bound has elapsed
// since the build was requested, since the timer cannot fire sooner; a raw
// cancellation before that came from outside the bound and passes through.
// A cancellation the executor already attributed to the caller or to an
// outside party keeps that attribution. pg-sprite typing its pre-build
// reads under the served budget as the same statement budget verdict
// retires the raw-code arm.
func nameConcurrentIndexBound(err error, bound, elapsed time.Duration) error {
	var budgetErr *executor.BudgetError
	if errors.As(err, &budgetErr) && budgetErr.Cause == executor.CauseStatement {
		return &concurrentIndexBoundError{bound: bound, err: err}
	}
	if errors.Is(err, executor.ErrCancelledByCaller) || errors.Is(err, executor.ErrCancelledExternally) {
		return err
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == sqlstateQueryCanceled && elapsed >= bound {
		return &concurrentIndexBoundError{bound: bound, err: err}
	}
	return err
}

// recoveryContextDetail prefixes an operator-facing detail with the abandoned
// invalid index a failed recovery was clearing, so a failure typed without
// the index — a lock budget lost to a build still holding the table, a pool
// too small for the recovery's extra session — still names the entry the
// retry acts on. A failure that is itself an invalid-index verdict never
// reaches here: that arm is decided first and already names the index. The
// prefix is kept short so the remedy's lead survives the narrowest operator
// surface, which truncates the detail from the tail.
func recoveryContextDetail(err error, detail string) string {
	var recoveryErr *indexRecoveryError
	if !errors.As(err, &recoveryErr) {
		return detail
	}
	return sanitizeReasonText(fmt.Sprintf("recovering abandoned invalid index %q.%q failed: %s",
		recoveryErr.verdict.Schema, recoveryErr.verdict.Index, detail))
}

// Progress reports phase and statement position for the apply
// the caller identifies via ResumeState.MigrationContext. Every accepted apply
// is tracked under its own identity, so a caller always reads its own schema
// change's state and never a sibling's — one engine is shared for the lifetime
// of a target, and answering with whichever apply wrote last would report
// another schema change's state, including a terminal one, for work that is
// still in flight. A caller asking about an apply the engine is not tracking
// gets the idle sentinel.
//
// While the apply runs, the step position and statement come from the
// pg-sprite tracker its executor feeds; the engine's own record only moves at
// accept and at the terminal outcome. The tracker read happens outside the
// engine lock: for an active concurrent index build it queries the server's
// progress view, and a poll must never hold up Apply or publishProgress on
// a database round trip. The coupling runs the other way too: the tracker
// serializes that query against the executor's own end-of-build fence, so
// a read still on the wire delays the apply's failure verdict and the
// terminal publish behind it. executorProgressMetadata bounds every read
// with the engine's own deadline so that delay is never open-ended.
func (e *Engine) Progress(ctx context.Context, req *engine.ProgressRequest) (*engine.ProgressResult, error) {
	var key string
	if req != nil {
		key = progressIdentity(req.ResumeState)
	}
	if key == "" {
		// The idle sentinel below means this engine tracks no such apply, which
		// a caller may act on as "nothing runs". A request that names no apply
		// has not asked about one, so it gets an error, never that answer.
		return nil, errMissingApplyIdentity("progress")
	}
	e.mu.Lock()
	tracked := e.progress[key]
	if tracked == nil {
		e.mu.Unlock()
		// The exact idle message is a cross-engine contract: stale-task
		// recovery compares against it verbatim to auto-resolve work
		// abandoned by a crashed server.
		return &engine.ProgressResult{State: engine.StatePending, Message: "No active schema change"}, nil
	}
	source := tracked.result
	result := *source
	result.Metadata = cloneMetadata(source.Metadata)
	result.Tables = cloneTables(source.Tables)
	tracker, logger := tracked.tracker, tracked.logger
	e.mu.Unlock()

	if result.State.IsTerminal() {
		// A terminal result already carries the executor's final position,
		// folded in when it was published.
		return &result, nil
	}
	if err := executorProgressMetadata(ctx, tracker, &result); err != nil {
		// The poll still answers with the last-known position: a progress
		// view the engine cannot read this instant is not a reason to tell
		// the driver its apply is unobservable.
		logger.Warn("PostgreSQL apply progress reports the last-known executor position",
			"task_id", key, "error", err)
	}
	e.retainRunningPercent(key, source, &result)
	return &result, nil
}

// retainRunningPercent carries a poll's derived percent forward on the
// running record it was cloned from, so the percent is part of the
// last-known position the next poll starts from. The server publishes a
// build row only while the build is in flight: a poll that lands between the
// executor's steps, after the build session is released, or on a tolerated
// read failure derives nothing, and without the write-back it would answer
// with the record's pre-execution zero. The write-back is skipped when the
// record was replaced while the tracker was being read, whether by a terminal
// publish or a re-claim. Both the floor and the write-back target the cloned
// record, so neither applies once that record is no longer the one the engine
// serves. This write is on the writer side of the UX-3 boundary: the drive
// loop's poll path mutates only the engine's in-memory record, while operator
// reads use stored rows that only the drive persists.
//
// The percent never regresses within an executor step. A build is one step —
// the executor's bounded retries apply to transactional statements, not to a
// build — and pg-sprite's percent only advances as the build moves through
// its phases, so a poll whose row derives a lower percent than the record
// carries read the server before a poller that has already carried a later
// row forward. The tracker serializes the reads but not the write-backs, so
// the earlier reading lands here second; it adopts the record's percent
// rather than pinning a position the build has left behind. A reading from a
// different step starts that step's own percent, and a poll that reports a
// step other than an index build removes the precise percent from the
// record.
func (e *Engine) retainRunningPercent(key string, source, result *engine.ProgressResult) {
	if len(result.Tables) == 0 {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	tracked := e.progress[key]
	if tracked == nil || tracked.result != source || len(source.Tables) == 0 {
		return
	}
	if !readsAnotherStep(result, tracked.percentStep) && runningPercentBehind(result, source) {
		result.Progress = source.Progress
		result.Tables[0].Progress = source.Tables[0].Progress
		if precise, ok := source.Metadata[runningPercentMetadataKey]; ok && !reportsStepOtherThanIndexBuild(result.Metadata) {
			result.Metadata[runningPercentMetadataKey] = precise
		}
		return
	}
	source.Progress = result.Progress
	source.Tables[0].Progress = result.Tables[0].Progress
	precise, ok := result.Metadata[runningPercentMetadataKey]
	switch {
	case ok:
		if source.Metadata == nil {
			source.Metadata = make(map[string]string)
		}
		source.Metadata[runningPercentMetadataKey] = precise
		tracked.percentStep = result.Metadata["step"]
	case reportsStepOtherThanIndexBuild(result.Metadata):
		delete(source.Metadata, runningPercentMetadataKey)
	}
}

// readsAnotherStep reports whether a poll read its precise percent from a
// different executor step than the one the record's percent came from. Each
// step's percent starts again from zero, so a second index build in the same
// apply is not held at the first build's final reading.
func readsAnotherStep(result *engine.ProgressResult, percentStep string) bool {
	if _, ok := result.Metadata[runningPercentMetadataKey]; !ok {
		return false
	}
	step := result.Metadata["step"]
	return percentStep != "" && step != "" && step != percentStep
}

// runningPercentBehind reports whether a poll's percent trails the running
// record's. Within one whole percent the precise percent decides, because two
// readings there differ only in their decimals. A poll that derived no precise
// percent of its own trails a record that carries one: it cloned the record
// before an overlapping poll stored that reading, so it adopts the reading
// rather than show the build as just starting.
func runningPercentBehind(result, source *engine.ProgressResult) bool {
	if result.Progress != source.Progress {
		return result.Progress < source.Progress
	}
	sourcePrecise, sourceOK := preciseRunningPercent(source.Metadata)
	if !sourceOK {
		return false
	}
	resultPrecise, resultOK := preciseRunningPercent(result.Metadata)
	if !resultOK {
		return true
	}
	return resultPrecise < sourcePrecise
}

// preciseRunningPercent reads back the precise percent setRunningPercent
// recorded. The engine writes the value itself, so one that does not parse
// is treated as absent rather than compared.
func preciseRunningPercent(metadata map[string]string) (float64, bool) {
	raw, ok := metadata[runningPercentMetadataKey]
	if !ok {
		return 0, false
	}
	percent, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, false
	}
	return percent, true
}

// executorProgressMetadata folds the tracker's current position and operation
// detail into the published result: the 1-based step the executor is running,
// the sequence length it announced, the statement text of that step, the
// executor's operation class, the attempt, and — while PostgreSQL publishes a
// progress row for a concurrent index build — the server's phase with its
// block, tuple and locker counters. Each key is written only once the
// executor has reported it, so before execution starts the metadata keeps
// progressResult's pre-execution position. The counters are the exception:
// once the server publishes a row every counter is written, zeros included,
// because a zero is a reading (no lockers cleared yet) and an absent key
// means the server has not published one. A running result's percent is the
// one pg-sprite derives for the step in flight; while pg-sprite reports none —
// no row, a phase it does not know, or a read that failed — the percent the
// result already carries stands. The statement passes through
// sanitizeStatementText and the server phase through sanitizeReasonText
// because the metadata is destined for operator-facing single-line rendering
// and is stored at a bounded width, and the value must satisfy both the
// moment one starts reading it.
//
// For an active concurrent index build the tracker queries the session the
// executor reserved for the build's failure verdict. The read runs on its
// own bounded context, detached from the caller's cancellation: a poller or
// drive context cancelled mid-query would otherwise tear down that session
// and leave the build's verdict indeterminate. The deadline starts before
// the tracker takes its observer lock, so the wait behind another observer's
// read counts against the same budget as the read itself — the poller is not
// always the apply's own driver, since another apply's conflict probe reads
// this tracker too, and a read parked behind one must not hold the
// executor's fence for longer than the engine allows any single read. A
// tracker read fails only when the server's progress view cannot be queried;
// the snapshot still carries the last-known position, which is written
// before the error is returned for the caller to log. Every tracked apply
// carries the tracker Apply created for it, so the caller never passes none.
func executorProgressMetadata(ctx context.Context, tracker buildTracker, result *engine.ProgressResult) error {
	if result.Metadata == nil {
		result.Metadata = make(map[string]string)
	}
	metadata := result.Metadata
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), executorProgressReadTimeout)
	defer cancel()
	snapshot, err := tracker.Progress(readCtx)
	if snapshot.TotalSteps > 0 {
		metadata["steps_total"] = strconv.Itoa(snapshot.TotalSteps)
	}
	if snapshot.Step > 0 {
		metadata["step"] = strconv.Itoa(snapshot.Step)
	}
	if statement := sanitizeStatementText(snapshot.Detail.Statement); statement != "" {
		metadata["statement"] = statement
	}
	if snapshot.Detail.Operation != "" {
		metadata["executor_operation"] = string(snapshot.Detail.Operation)
	}
	if serverPhase := sanitizeReasonText(snapshot.Detail.ServerPhase); serverPhase != "" {
		metadata["server_phase"] = serverPhase
	}
	if snapshot.Detail.Attempt > 0 {
		metadata["attempt"] = strconv.Itoa(snapshot.Detail.Attempt)
	}
	if work := snapshot.Detail.Work; work != nil {
		metadata["blocks_done"] = strconv.FormatUint(work.BlocksDone, 10)
		metadata["blocks_total"] = strconv.FormatUint(work.BlocksTotal, 10)
		metadata["tuples_done"] = strconv.FormatUint(work.TuplesDone, 10)
		metadata["tuples_total"] = strconv.FormatUint(work.TuplesTotal, 10)
		metadata["lockers_done"] = strconv.FormatUint(work.LockersDone, 10)
		metadata["lockers_total"] = strconv.FormatUint(work.LockersTotal, 10)
	}
	// A step waiting outside the lock queue for a conflicting session to end
	// names that session, so an operator can see what the apply waits on.
	// The keys are absent once the wait ends.
	if wait := snapshot.Detail.LockHolderWait; wait != nil {
		metadata[apitypes.LockHolderPIDMetadataKey] = strconv.FormatUint(uint64(wait.PID), 10)
		if table := sanitizeReasonText(wait.Table); table != "" {
			metadata[apitypes.LockHolderTableMetadataKey] = table
		}
		metadata[apitypes.LockHolderWaitedMetadataKey] = strconv.FormatInt(wait.Waited.Milliseconds(), 10)
		metadata[apitypes.LockHolderLimitMetadataKey] = strconv.FormatInt(wait.Limit.Milliseconds(), 10)
	} else {
		for _, key := range apitypes.LockHolderMetadataKeys {
			delete(metadata, key)
		}
	}
	// pg-sprite's percent describes work in flight, so it only refines a
	// running result's percent. A terminal result's percent is decided by its
	// state; a stale build snapshot must not pull a completed apply below 100.
	// Only a concurrent index build's percent becomes the apply's: pg-sprite
	// also measures some other steps, such as a copy, by their own completion,
	// and one step's completion is not the apply's.
	if percent := snapshot.Detail.Percent; percent != nil && snapshot.Detail.Operation == progress.OperationConcurrentIndex && !result.State.IsTerminal() {
		setRunningPercent(result, *percent)
	}
	if result.State.IsTerminal() || reportsStepOtherThanIndexBuild(metadata) {
		delete(metadata, runningPercentMetadataKey)
	}
	if err != nil {
		return fmt.Errorf("read pg-sprite executor progress: %w", err)
	}
	return nil
}

// runningPercentMetadataKey is the metadata key that carries pg-sprite's
// percent for a running index build with its decimals, so a large build
// visibly moves between polls while its whole percent stands still. It is
// present only while the executor reports a concurrent index build as the
// step in flight, and it is removed once another step or a terminal state is
// reported.
const runningPercentMetadataKey = "percent"

// reportsStepOtherThanIndexBuild reports whether the executor names a step in
// flight that is not a concurrent index build. A poll that names no step has
// no reading of its own and keeps the record's last-known position.
func reportsStepOtherThanIndexBuild(metadata map[string]string) bool {
	operation, ok := metadata["executor_operation"]
	return ok && operation != string(progress.OperationConcurrentIndex)
}

// setRunningPercent writes pg-sprite's percent for the running step onto both
// the apply and its single table, so the two never disagree, and records it
// with two decimals in the metadata. Both are rounded down, the decimals to
// hundredths, so a running step never reads as finished whatever precision
// pg-sprite reports it with. The small offset keeps a value pg-sprite already
// rounded to hundredths, such as 21.88, from flooring to 21.87 because its
// binary form sits just below it.
func setRunningPercent(result *engine.ProgressResult, percent float64) {
	if len(result.Tables) == 0 {
		return
	}
	percent = math.Floor(percent*100+1e-6) / 100
	whole := int(math.Floor(percent))
	result.Progress = whole
	result.Tables[0].Progress = whole
	if result.Metadata == nil {
		result.Metadata = make(map[string]string)
	}
	result.Metadata[runningPercentMetadataKey] = strconv.FormatFloat(percent, 'f', 2, 64)
}

// errMissingApplyIdentity is the error for a request that does not name the
// apply it is about. The engine tracks each apply under its identity, so it
// cannot answer for work the request does not name, and answering "nothing
// runs" could let a caller record a schema change as ended while it runs.
func errMissingApplyIdentity(operation string) error {
	return fmt.Errorf("PostgreSQL %s request names no apply: ResumeState.MigrationContext is empty, so the engine cannot tell whether a schema change is running", operation)
}

// progressIdentity extracts the apply identity that keys engine progress.
// The drive layer stamps the task identifier into
// ResumeState.MigrationContext on both Apply and Progress requests, so the
// two sides of the comparison come from the same source.
func progressIdentity(rs *engine.ResumeState) string {
	if rs == nil {
		return ""
	}
	return rs.MigrationContext
}

func progressResult(state engine.State, phase string, started time.Time, change nativeApply, detail string) *engine.ProgressResult {
	progress := 0
	if state == engine.StateCompleted {
		progress = 100
	}
	steps := change.steps
	if steps == 0 {
		steps = 1
	}
	result := &engine.ProgressResult{
		State: state, Progress: progress, Message: "PostgreSQL schema change " + phase,
		ErrorMessage: detail,
		Metadata: map[string]string{
			"phase": phase,
			// The position the record carries before the executor has
			// reported one: the first step of the planned sequence, with the
			// total taken from the plan. executorProgressMetadata replaces
			// each key as the tracker reports it — the total once the
			// executor announces its sequence, the step once it starts one —
			// so through the executor's admission checks the record still
			// shows the planned first step.
			"step": "1", "steps_total": strconv.Itoa(steps),
		},
		Tables: []engine.TableProgress{{
			Namespace: change.namespace, Table: change.table, DDL: change.sql,
			State: string(state), Progress: progress, StartedAt: &started,
		}},
	}
	if state.IsTerminal() {
		completed := time.Now()
		result.Tables[0].CompletedAt = &completed
		result.Metadata["elapsed_ms"] = strconv.FormatInt(completed.Sub(started).Milliseconds(), 10)
	}
	return result
}

// claimProgress starts tracking an accepted apply. Only Apply calls this:
// acceptance is the moment the engine becomes answerable for a schema change's
// progress.
//
// Accepting an apply also retires the entries that already reached a terminal
// state. A terminal entry is kept only so the driver polling that apply can
// read its outcome, and a driver that has accepted another apply on this target
// has moved past it; anything still polling a retired identity reads the idle
// sentinel and settles against the target schema, which is authoritative.
// Entries for applies that are still running are never retired, so an
// in-flight change always answers for itself no matter how many siblings the
// engine accepts.
//
// The returned channel is the drive's to close once its terminal result is
// published; Cancel waits on it for the outcome.
func (e *Engine) claimProgress(key string, result *engine.ProgressResult, tracker *progress.Tracker, logger *slog.Logger, shape buildShape, cancelApply context.CancelFunc, owner string) chan struct{} {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.progress == nil {
		e.progress = make(map[string]*trackedApply)
	}
	for id, tracked := range e.progress {
		if id != key && tracked.result.State.IsTerminal() {
			delete(e.progress, id)
		}
	}
	done := make(chan struct{})
	e.progress[key] = &trackedApply{
		result: result, tracker: tracker, logger: logger,
		cancellableBuild: shape.cancellable, partitionedBuild: shape.partitioned, haltable: shape.haltable, validationBound: shape.validationBound, cancelApply: cancelApply, done: done,
		owner: owner,
	}
	return done
}

// publishProgress stores a background apply's progress unless the engine has
// stopped tracking that apply. Drain is the only writer that stops tracking a
// running apply, and it means the drive that accepted the work has given it up,
// so the write is logged and discarded rather than resurrecting an entry no
// poller is waiting for.
func (e *Engine) publishProgress(key string, result *engine.ProgressResult, logger *slog.Logger) {
	e.mu.Lock()
	defer e.mu.Unlock()
	tracked, ok := e.progress[key]
	if !ok {
		logger.Warn("PostgreSQL apply progress discarded: the engine no longer tracks this schema change",
			"task_id", key, "state", result.State)
		return
	}
	tracked.result = result
}

func cloneMetadata(metadata map[string]string) map[string]string {
	cloned := make(map[string]string, len(metadata))
	maps.Copy(cloned, metadata)
	return cloned
}

// cloneTables deep-copies table progress so callers can never mutate the
// engine's stored progress through the returned slice, its time pointers, or
// its shard breakdowns.
func cloneTables(tables []engine.TableProgress) []engine.TableProgress {
	if tables == nil {
		return nil
	}
	cloned := slices.Clone(tables)
	for i := range cloned {
		if cloned[i].StartedAt != nil {
			startedAt := *cloned[i].StartedAt
			cloned[i].StartedAt = &startedAt
		}
		if cloned[i].CompletedAt != nil {
			completedAt := *cloned[i].CompletedAt
			cloned[i].CompletedAt = &completedAt
		}
		cloned[i].Shards = slices.Clone(cloned[i].Shards)
	}
	return cloned
}
