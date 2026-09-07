package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/block/pg-sprite/pkg/dbconn"
	"github.com/block/pg-sprite/pkg/executor"
	"github.com/block/pg-sprite/pkg/preflight"
	pgstatement "github.com/block/pg-sprite/pkg/statement"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
)

const (
	optimisticLockTimeout    = 3 * time.Second
	optimisticStatementLimit = 30 * time.Second

	// optimisticApplyCeiling bounds one whole background apply client-side:
	// pool dial, preflight, and every execution attempt with its backoff.
	// Server-side statement/lock timeouts bound queries once a session is
	// healthy, but they cannot unwedge a hung dial or a black-holed
	// connection — the ceiling guarantees the drive always reaches a
	// terminal progress state. Above the worst legitimate run on either
	// execution path — generously above the retry path's (retry attempts x
	// statement limit plus backoffs), and above the concurrent index
	// budget with enough headroom for session setup and the post-failure
	// catalog verdict — so it only fires on genuine hangs.
	optimisticApplyCeiling = 5 * time.Minute

	// concurrentIndexBudget bounds one CREATE INDEX CONCURRENTLY build.
	// Concurrent builds get their own budget instead of the per-statement
	// limit: their snapshot waits are lock waits by implementation, so the
	// executor runs them with lock_timeout disabled under one overall
	// statement deadline. Deliberately below the apply ceiling so the
	// server-side deadline fires before the client-side ceiling cancels
	// the session — exhaustion then surfaces as the typed budget verdict
	// rather than an ambiguous external cancellation.
	concurrentIndexBudget = 4 * time.Minute

	// concurrentIndexHeadroom is the least the apply ceiling must exceed
	// the concurrent index budget by. The ceiling wraps the whole apply,
	// so the gap has to absorb everything that shares the ceiling with the
	// build — pool dial, the privilege check, the preflight table read, the
	// partition admission facts lookup, and the rest of the session setup
	// executeOptimistic runs before the build — and, after the budget has
	// already expired, the catalog verdict that names an invalid leftover
	// index. If the ceiling fires first the verdict has no live context to
	// run in and the failure degrades to an external cancellation that
	// names no index, which is the outcome the verdict exists to prevent.
	concurrentIndexHeadroom = time.Minute
)

type nativeApply struct {
	namespace string
	table     string
	sql       string
	steps     int
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

	started := time.Now()
	key := progressIdentity(req.ResumeState)
	e.claimProgress(key, progressResult(engine.StateRunning, "preflight", started, change, ""))
	bgCtx := context.WithoutCancel(ctx)
	conn := targetConn{dsn: req.Credentials.DSN, caCertPath: caPath}
	logger := req.Logger
	if logger == nil {
		logger = slog.Default()
	}
	e.wg.Go(func() { e.runOptimisticApply(bgCtx, conn, change, key, started, logger) })

	return &engine.ApplyResult{
		Accepted:    true,
		Message:     "Started native-safe PostgreSQL schema change",
		ResumeState: req.ResumeState,
	}, nil
}

// validateOptimisticApply validates only engine-level applicability. Blocked
// verdicts are enforced at queue time by rejectBlockedStoredPlan before tasks
// are created; task rows are rebuilt from stored plans and do not carry the
// plan's ExecutionMode verdict.
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
	if req.Options["defer_cutover"] == "true" {
		return nativeApply{}, fmt.Errorf("apply PostgreSQL table %q: deferred cutover is unsupported", tc.Table)
	}
	// The same tier derivation runs again inside the background apply; this
	// synchronous check exists so a statement shape the native-safe path
	// cannot execute is refused at acceptance, before any work is queued.
	statements, err := postgresCreateSetStatements(tc.DDL)
	if err != nil {
		return nativeApply{}, fmt.Errorf("apply PostgreSQL table %q: planned DDL is not one statement or a valid greenfield create set", tc.Table)
	}
	if _, err := preflight.RequiredTier(statements); err != nil {
		return nativeApply{}, fmt.Errorf("apply PostgreSQL table %q: SchemaBot's PostgreSQL support does not execute this statement shape yet", tc.Table)
	}
	return nativeApply{namespace: req.Changes[0].Namespace, table: tc.Table, sql: tc.DDL, steps: len(statements)}, nil
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

func (e *Engine) runOptimisticApply(ctx context.Context, conn targetConn, change nativeApply, key string, started time.Time, logger *slog.Logger) {
	// The context arrives detached from the caller (an accepted apply must
	// survive the request), so boundedness comes from the ceiling instead.
	ctx, cancel := context.WithTimeout(ctx, optimisticApplyCeiling)
	defer cancel()
	err := executeOptimistic(ctx, conn, change, e.tableSizeLimit)
	if err == nil {
		e.publishProgress(key, progressResult(engine.StateCompleted, "completed", started, change, ""), logger)
		return
	}

	var invalidErr *executor.InvalidIndexError
	if errors.As(err, &invalidErr) {
		// An invalid index — pre-existing or a build's own unrecovered
		// leftover — is operational: an operator clears it and a retry can
		// succeed. Checked before the refusal and budget arms because the
		// verdict wraps the build failure that produced it (a budget-
		// cancelled build leaves its own invalid index), and that inner
		// cause must not be read as the outcome — the index the operator
		// clears is. The detail is built from the typed identifiers and
		// verdict code, never the wrapped build or cleanup errors, which
		// may carry raw server text; the full cause lands in the server
		// log below it.
		logger.Error("PostgreSQL concurrent index build left or found an invalid index",
			"namespace", change.namespace, "table", change.table,
			"index_schema", invalidErr.Schema, "index", invalidErr.Index, "error", err)
		result := progressResult(engine.StateFailed, "failed", started, change,
			invalidIndexDetail(invalidErr))
		result.Retryable = true
		e.publishProgress(key, result, logger)
		return
	}

	if r := classifyRefusal(err, change.table); r != nil {
		// The taxonomy's reason survives in the operator-facing detail; no
		// metadata carries it because nothing downstream consumes one yet.
		e.publishProgress(key, progressResult(engine.StateFailed, "refused", started, change, r.detail), logger)
		return
	}

	failure := classifyApplyFailure(err, change.table)
	switch {
	case failure.committedPrefix:
		logger.Error("PostgreSQL create set failed after committing a prefix",
			"namespace", change.namespace, "table", change.table, "error", err)
	case failure.lockBudget:
		logger.Warn("PostgreSQL native-safe schema change lost its lock budget; the drive will retry",
			"namespace", change.namespace, "table", change.table, "error", err)
	default:
		logger.Error("PostgreSQL native-safe schema change failed", "namespace", change.namespace, "table", change.table, "error", err)
	}
	result := progressResult(engine.StateFailed, "failed", started, change, failure.detail)
	result.Retryable = failure.retryable
	e.publishProgress(key, result, logger)
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
// blocked would falsely tell the operator retrying cannot succeed.
func classifyApplyFailure(err error, table string) applyFailure {
	if detail, committed := committedCreatePrefixDetail(err, table); committed {
		return applyFailure{detail: detail, committedPrefix: true}
	}
	var budgetErr *executor.BudgetError
	if errors.As(err, &budgetErr) {
		return applyFailure{
			detail:     budgetErr.Error() + "; retry once lock contention subsides",
			retryable:  true,
			lockBudget: true,
		}
	}
	return applyFailure{detail: "PostgreSQL schema change failed; see server logs", retryable: true}
}

// invalidIndexDetail renders the operator-facing next step for an
// invalid-index verdict, matching pg-sprite's own ownership standard: a drop
// is named only when the entry is proven this build's own leftover. A
// pre-existing invalid entry may be another actor's still-running build, and
// an unproven verdict may sit on a healthy index — both get investigation
// steps, never a statement to run. Only the typed identifiers are
// interpolated, never the wrapped build or cleanup errors, which may carry
// raw server text.
func invalidIndexDetail(invalidErr *executor.InvalidIndexError) string {
	name := fmt.Sprintf("%q.%q", invalidErr.Schema, invalidErr.Index)
	var advice string
	switch invalidErr.Code() {
	case executor.CodeInvalidIndexOwnLeftover:
		advice = fmt.Sprintf("this build left its own invalid index %s on the target; drop the invalid index, then retry", name)
	case executor.CodeInvalidIndexPreexisting:
		advice = fmt.Sprintf("an invalid index %s already occupies the name on the target and may be another actor's build still in progress; check pg_stat_activity before any recovery, then retry", name)
	default:
		// CodeInvalidIndexUnproven and any future verdict fail safe with
		// investigation steps: the index under the name may be healthy.
		advice = fmt.Sprintf("index %s may be invalid but its catalog state could not be verified; inspect pg_index.indisvalid on the target before any recovery, then retry", name)
	}
	return sanitizeReasonText(advice)
}

// refusal is a typed apply outcome that retrying cannot fix: the schema
// change, the target table, or role provisioning must change first.
//
// cause names what failed and remedy names what the operator changes; they
// are kept apart so the failed sequence step can be placed between them
// without parsing the rendered text. detail is the operator-facing line
// classifyRefusal composes from them — the only field consumers read.
type refusal struct {
	reason string
	cause  string
	remedy string
	detail string
}

// classifyRefusal maps pg-sprite's typed refusal inputs to permanent
// refusals, for both the plan-time privilege check and the apply path — one
// classifier so the same underlying failure reads identically on both
// surfaces. A nil result means the failure is operational — a retry may
// succeed once conditions change. Lock-budget exhaustion is deliberately
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
	clauses := []string{r.cause}
	remedy := r.remedy
	var stepErr *executor.SequenceStepError
	if errors.As(err, &stepErr) && stepErr.Total > 1 {
		clauses = append(clauses, sequenceStepClause(stepErr, table))
		if stepErr.Step > 1 && remedy == "" {
			remedy = replanRemedy
		}
	}
	if remedy != "" {
		clauses = append(clauses, remedy)
	}
	r.detail = sanitizeReasonText(strings.Join(clauses, "; "))
	return r
}

const replanRemedy = "re-plan against the current schema"

// sequenceStepClause names the failed step of a multi-statement create set
// and, past the first step, the CREATE TABLE that step left committed.
func sequenceStepClause(stepErr *executor.SequenceStepError, table string) string {
	clause := fmt.Sprintf("step %d of %d failed", stepErr.Step, stepErr.Total)
	if stepErr.Step > 1 {
		clause += fmt.Sprintf(" after the CREATE TABLE for %q committed", table)
	}
	return clause
}

// committedCreatePrefixDetail reports the non-retryable recovery action for a
// create sequence that failed after at least one earlier step committed.
func committedCreatePrefixDetail(err error, table string) (string, bool) {
	var stepErr *executor.SequenceStepError
	if !errors.As(err, &stepErr) || stepErr.Step <= 1 {
		return "", false
	}
	detail := sequenceStepClause(stepErr, table) + "; " + replanRemedy
	return sanitizeReasonText(detail), true
}

// refusalForCause holds classifyRefusal's cause-to-refusal mapping; causes
// and remedies leave unsanitized and classifyRefusal sanitizes the composed
// detail at its return.
func refusalForCause(err error, table string) *refusal {
	var privilegeErr *preflight.PrivilegeError
	if errors.As(err, &privilegeErr) {
		object := fmt.Sprintf("on table %q", table)
		if privilegeErr.Tier == preflight.TierCreateTable {
			// The create tier's grant is schema-scoped: the table does not
			// exist yet, so pointing the operator at a table-level grant
			// would send them hunting for an object the target lacks.
			object = fmt.Sprintf("in the schema that would hold table %q", table)
		}
		remedy := fmt.Sprintf("provision with: %s (verified by: %s)", privilegeErr.Grant, privilegeErr.Check)
		if privilegeErr.Hint != "" {
			remedy += "; " + privilegeErr.Hint
		}
		return &refusal{reason: "insufficient-privileges",
			cause:  fmt.Sprintf("the engine role lacks access for %s %s", privilegeErr.Tier, object),
			remedy: remedy}
	}
	// An invalid-index verdict is operational even when the build failure it
	// wraps would classify as a refusal on its own — a budget-cancelled
	// concurrent build leaves its own invalid index, and the index the
	// operator clears is the outcome, not the inner budget exhaustion.
	// Declined before the budget arm so the nested cause can never shadow
	// the verdict.
	var invalidErr *executor.InvalidIndexError
	if errors.As(err, &invalidErr) {
		return nil
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
	var sizeErr *preflight.SizeError
	if errors.As(err, &sizeErr) {
		// Name whose limit this is: read cold, the size text sounds like a
		// property of PostgreSQL or of the change, when it is SchemaBot's
		// own conservatism for the native-safe path.
		return &refusal{reason: "table-too-large",
			cause: sizeErr.Error() + "; this threshold is SchemaBot's ceiling for a native-safe apply, not a PostgreSQL limit"}
	}
	if errors.Is(err, preflight.ErrTableNotFound) {
		return tableNotFoundRefusal(table)
	}
	if errors.Is(err, preflight.ErrNotTable) {
		return &refusal{reason: "not-a-table",
			cause: fmt.Sprintf("%q exists but is not an ordinary or partitioned table", table)}
	}
	if preflight.IsNameOccupied(err) {
		return createCollisionRefusal(table)
	}
	if errors.Is(err, preflight.ErrSchemaNotFound) {
		return &refusal{reason: "schema-not-found",
			cause:  fmt.Sprintf("the schema that would hold table %q does not exist on the target", table),
			remedy: "create the schema first"}
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
	case executor.CodeDuplicateCreateName:
		return &refusal{reason: "duplicate-create-name",
			cause:  fmt.Sprintf("the create set for %q claims the same relation name twice (a CREATE INDEX name repeats the table's implicit constraint-index name or another index)", table),
			remedy: "fix the schema file and re-plan"}, true
	case executor.CodePartitionOfUnsupported:
		return &refusal{reason: "unsupported-create-step",
			cause: fmt.Sprintf("the CREATE TABLE for %q attaches a partition to a live parent, which the native-safe create path does not run", table)}, true
	case executor.CodeIfNotExistsUnsupported:
		return &refusal{reason: "unsupported-create-step",
			cause:  fmt.Sprintf("the planned statement for %q carries IF NOT EXISTS, whose no-op outcome the native-safe path cannot prove", table),
			remedy: "drop the clause and re-plan"}, true
	case executor.CodeUnsupportedCreateStep:
		return &refusal{reason: "unsupported-create-step",
			cause:  fmt.Sprintf("the CREATE TABLE for %q is not a shape the native-safe create path can run", table),
			remedy: "rewrite the schema file and re-plan"}, true
	case executor.CodeTableNotFound:
		return tableNotFoundRefusal(table), true
	case executor.CodeEmptySequence, executor.CodeUnsupportedSequenceStep,
		executor.CodeUnsupportedPartitionedParent, executor.CodeNotConcurrentIndexBuild,
		executor.CodeUnnamedIndex, executor.CodeUnqualifiedTable:
		// Shape refusals: the executor refused the statement's form at
		// admission, so retrying the identical plan refails the same way.
		return &refusal{reason: "unsupported-statement-shape",
			cause:  fmt.Sprintf("the planned statement for %q is not a shape the native-safe path can run", table),
			remedy: "rewrite the schema change and re-plan"}, true
	case executor.CodeBudgetStatementExceeded:
		// Normally consumed upstream by the typed BudgetError arm, which
		// renders the budget's own figures; this mapping keeps the outcome
		// vocabulary total.
		return &refusal{reason: "not-native-safe-budget-exceeded",
			cause: fmt.Sprintf("the statement for table %q ran past its statement budget and was cancelled", table)}, true
	case executor.CodeInvariantViolation:
		// Never a retry candidate per the executor's contract: an invariant
		// breach means the engine's own safety accounting failed, so the
		// apply fails closed until an operator has inspected the target.
		return &refusal{reason: "engine-invariant-violation",
			cause:  fmt.Sprintf("the engine's safety invariants did not hold while changing table %q", table),
			remedy: "inspect the target and server logs before re-running"}, true
	case executor.CodeBudgetLockExceeded, executor.CodeCancelledExternally,
		executor.CodeInvalidIndexOwnLeftover, executor.CodeInvalidIndexPreexisting,
		executor.CodeInvalidIndexUnproven, executor.CodePoolTooSmall,
		executor.CodeExecutionFailed:
		// Operational outcomes: a bounded lock race, an external stop, an
		// invalid-index state an operator clears, engine pool sizing, or a
		// failure outside the typed set. A retry can succeed once
		// conditions change, so none is a permanent refusal.
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

// createCollisionRefusal names the kinds of name a create set claims so the
// operator knows where to look; the server error identifying the occupant
// stays in the logs. The wording is kept short because the composed detail
// must survive the narrowest operator surface with its remedy intact.
func createCollisionRefusal(table string) *refusal {
	return &refusal{
		reason: "create-collision",
		cause:  fmt.Sprintf("a name the create set for %q needs is already occupied (table, index, constraint, or sequence)", table),
		remedy: createCollisionRemedy,
	}
}

func tableNotFoundRefusal(table string) *refusal {
	return &refusal{
		reason: "table-not-found",
		cause:  fmt.Sprintf("table %q does not exist on the target", table),
		remedy: replanRemedy,
	}
}

func executeOptimistic(ctx context.Context, conn targetConn, change nativeApply, tableSizeLimit int64) error {
	poolCfg, err := spritePoolConfig(conn.dsn, conn.caCertPath)
	if err != nil {
		return fmt.Errorf("prepare pg-sprite apply pool for table %q: %w", change.table, err)
	}
	pool, err := dbconn.NewPool(ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("open pg-sprite apply pool: %w", err)
	}
	defer pool.Close()

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
		return executeCreate(ctx, pool, change, statements)
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
	table, err := preflight.CheckTable(ctx, pool, change.namespace, change.table, tableSizeLimit)
	if err != nil {
		return fmt.Errorf("preflight PostgreSQL table %q: %w", change.table, err)
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
	if statement.Kind() == pgstatement.KindCreateIndex && statement.Concurrent() {
		// A concurrent build cannot run inside a transaction block, so it
		// must not reach the transactional optimistic executor: pg-sprite's
		// dedicated index-build executor runs it under the CONCURRENTLY
		// budget policy and returns a catalog-verified verdict — including
		// the invalid-index recovery a failed build needs.
		if _, err := executor.BuildIndexConcurrently(ctx, pool, change.sql,
			executor.ConcurrentBudget{Overall: concurrentIndexBudget}); err != nil {
			return fmt.Errorf("build PostgreSQL index concurrently on table %q: %w", change.table, err)
		}
		return nil
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
	// plan time or, when the rewrite could not be constructed, blocked; a
	// partitioned parent's build is refused by CheckPartitionSupport above;
	// and an index declared with a new table diverts to executeCreate,
	// where greenfieldCreateSet keeps the blocking form on purpose — a
	// table born in the run has no readers, and CONCURRENTLY cannot run
	// inside its create sequence.
	if err := executor.ExecuteNative(ctx, pool, table, statement, executor.Budget{
		LockTimeout: optimisticLockTimeout, StatementTimeout: optimisticStatementLimit,
	}, executor.DefaultRetryPolicy()); err != nil {
		return fmt.Errorf("execute native-safe PostgreSQL statement on table %q: %w", change.table, err)
	}
	return nil
}

// executeCreate runs a greenfield create set through pg-sprite's create path:
// parse the table and its declared indexes, prove the role can create in the schema,
// prove the name is free, then execute — both proofs minted here, in the
// session that executes, because absence or privilege at plan time proves
// nothing about apply time. The table size gate deliberately does not run:
// it bounds rewrites of existing data, and a table that does not exist yet
// has none.
func executeCreate(ctx context.Context, pool *pgxpool.Pool, change nativeApply, statements []string) error {
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
	role, err := preflight.CheckCreatePrivileges(ctx, pool, change.namespace)
	if err != nil {
		return fmt.Errorf("check creation access in PostgreSQL schema %q: %w", change.namespace, err)
	}
	absent, err := preflight.CheckTableAbsent(ctx, pool, change.namespace, change.table)
	if err != nil {
		return fmt.Errorf("verify PostgreSQL table %q is absent: %w", change.table, err)
	}
	if _, err := executor.ExecuteCreate(ctx, pool, absent, role, desired, executor.Budget{
		LockTimeout: optimisticLockTimeout, StatementTimeout: optimisticStatementLimit,
	}, executor.DefaultRetryPolicy()); err != nil {
		return fmt.Errorf("execute PostgreSQL CREATE TABLE %q: %w", change.table, err)
	}
	return nil
}

// Progress reports phase, elapsed time, and statement position for the apply
// the caller identifies via ResumeState.MigrationContext. Every accepted apply
// is tracked under its own identity, so a caller always reads its own schema
// change's state and never a sibling's — one engine is shared for the lifetime
// of a target, and answering with whichever apply wrote last would report
// another schema change's state, including a terminal one, for work that is
// still in flight. A caller asking about an apply the engine is not tracking
// gets the idle sentinel. Rich server progress is intentionally absent until
// the PostgreSQL executor exposes it.
func (e *Engine) Progress(_ context.Context, req *engine.ProgressRequest) (*engine.ProgressResult, error) {
	var key string
	if req != nil {
		key = progressIdentity(req.ResumeState)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	tracked := e.progress[key]
	if tracked == nil {
		// The exact idle message is a cross-engine contract: stale-task
		// recovery compares against it verbatim to auto-resolve work
		// abandoned by a crashed server.
		return &engine.ProgressResult{State: engine.StatePending, Message: "No active schema change"}, nil
	}
	result := *tracked
	result.Metadata = cloneMetadata(tracked.Metadata)
	result.Tables = cloneTables(tracked.Tables)
	if len(result.Tables) > 0 && result.Tables[0].StartedAt != nil && !result.State.IsTerminal() {
		result.Metadata["elapsed"] = time.Since(*result.Tables[0].StartedAt).Round(time.Millisecond).String()
	}
	return &result, nil
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
			"phase": phase, "elapsed": time.Since(started).Round(time.Millisecond).String(),
			// Per-step position is deliberately not tracked yet: the apply
			// publishes progress only at accept and at the terminal outcome,
			// and nothing observes the executor's step transitions in
			// between, so the position stays at the sequence's first step
			// while the total reports the real create-set length.
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
func (e *Engine) claimProgress(key string, result *engine.ProgressResult) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.progress == nil {
		e.progress = make(map[string]*engine.ProgressResult)
	}
	for tracked, progress := range e.progress {
		if tracked != key && progress.State.IsTerminal() {
			delete(e.progress, tracked)
		}
	}
	e.progress[key] = result
}

// publishProgress stores a background apply's progress unless the engine has
// stopped tracking that apply. Drain is the only writer that stops tracking a
// running apply, and it means the drive that accepted the work has given it up,
// so the write is logged and discarded rather than resurrecting an entry no
// poller is waiting for.
func (e *Engine) publishProgress(key string, result *engine.ProgressResult, logger *slog.Logger) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, tracked := e.progress[key]; !tracked {
		logger.Warn("PostgreSQL apply progress discarded: the engine no longer tracks this schema change",
			"task_id", key, "state", result.State)
		return
	}
	e.progress[key] = result
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
