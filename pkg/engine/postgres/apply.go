package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"time"

	"github.com/block/pg-sprite/pkg/dbconn"
	"github.com/block/pg-sprite/pkg/executor"
	"github.com/block/pg-sprite/pkg/preflight"
	pgstatement "github.com/block/pg-sprite/pkg/statement"

	"github.com/block/schemabot/pkg/engine"
)

const (
	optimisticLockTimeout    = 3 * time.Second
	optimisticStatementLimit = 30 * time.Second

	// optimisticApplyCeiling bounds one whole background apply client-side:
	// pool dial, preflight, and every execution attempt with its backoff.
	// Server-side statement/lock timeouts bound queries once a session is
	// healthy, but they cannot unwedge a hung dial or a black-holed
	// connection — the ceiling guarantees the drive always reaches a
	// terminal progress state. Generously above the worst legitimate run
	// (retry attempts x statement limit plus backoffs) so it only fires on
	// genuine hangs.
	optimisticApplyCeiling = 5 * time.Minute
)

type nativeApply struct {
	namespace string
	table     string
	sql       string
}

// targetConn carries one background apply's connection inputs: the raw DSN
// (normalized at open time) and the CA bundle path the pool verifies the
// target against — empty when the embedded RDS trust or the DSN's own
// settings apply.
type targetConn struct {
	dsn        string
	caCertPath string
}

// Apply starts one native-safe PostgreSQL statement under pg-sprite's bounded
// optimistic executor. Planner-produced sequences use the same execution seam
// but are deliberately not admitted by this first increment.
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

func validateOptimisticApply(req *engine.ApplyRequest) (nativeApply, error) {
	if req == nil {
		return nativeApply{}, fmt.Errorf("apply PostgreSQL schema: request is required")
	}
	if req.Credentials == nil || req.Credentials.DSN == "" {
		return nativeApply{}, fmt.Errorf("apply PostgreSQL database %q: DSN credentials are required", req.Database)
	}
	if len(req.Changes) != 1 || len(req.Changes[0].TableChanges) != 1 {
		return nativeApply{}, fmt.Errorf("apply PostgreSQL database %q: native-safe increment requires exactly one planned statement", req.Database)
	}
	tc := req.Changes[0].TableChanges[0]
	if tc.ExecutionMode == engine.ExecutionModeBlocked {
		return nativeApply{}, fmt.Errorf("apply PostgreSQL table %q: planned statement is blocked: %s", tc.Table, tc.ModeReason)
	}
	if req.Options["defer_cutover"] == "true" {
		return nativeApply{}, fmt.Errorf("apply PostgreSQL table %q: deferred cutover is unsupported", tc.Table)
	}
	// The same tier derivation runs again inside the background apply; this
	// synchronous check exists so a statement shape the native-safe path
	// cannot execute is refused at acceptance, before any work is queued.
	if _, err := preflight.RequiredTier([]string{tc.DDL}); err != nil {
		return nativeApply{}, fmt.Errorf("apply PostgreSQL table %q: SchemaBot's PostgreSQL support does not execute this statement shape yet", tc.Table)
	}
	return nativeApply{namespace: req.Changes[0].Namespace, table: tc.Table, sql: tc.DDL}, nil
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

	if r := classifyRefusal(err, change.table); r != nil {
		// The taxonomy's reason survives in the operator-facing detail; no
		// metadata carries it because nothing downstream consumes one yet.
		e.publishProgress(key, progressResult(engine.StateFailed, "refused", started, change, r.detail), logger)
		return
	}

	var budgetErr *executor.BudgetError
	if errors.As(err, &budgetErr) {
		// classifyRefusal consumed the statement-budget cause, so the budget
		// exceeded here is the lock budget: the statement itself is
		// native-safe and merely lost a bounded race with concurrent lock
		// holders. Surface the typed detail as a retryable failure — marking
		// it blocked would falsely tell the operator retrying cannot succeed.
		result := progressResult(engine.StateFailed, "failed", started, change,
			budgetErr.Error()+"; retry once lock contention subsides")
		result.Retryable = true
		e.publishProgress(key, result, logger)
		return
	}

	logger.Error("PostgreSQL native-safe schema change failed", "namespace", change.namespace, "table", change.table, "error", err)
	result := progressResult(engine.StateFailed, "failed", started, change, "PostgreSQL schema change failed; see server logs")
	// Operational failures are retryable by definition: classifyRefusal
	// returned nil, so nothing about the plan, target, or provisioning makes
	// a retry futile — the drive must not cancel the apply's remaining work.
	result.Retryable = true
	e.publishProgress(key, result, logger)
}

// refusal is a typed apply outcome that retrying cannot fix: the schema
// change, the target table, or role provisioning must change first.
type refusal struct {
	reason string
	detail string
}

// classifyRefusal maps pg-sprite's typed refusal inputs to permanent
// refusals, for both the plan-time privilege check and the apply path — one
// classifier so the same underlying failure reads identically on both
// surfaces. A nil result means the failure is operational — a retry may
// succeed once conditions change. Lock-budget exhaustion is deliberately
// operational: the statement is native-safe and only lost a bounded race
// with concurrent lock holders. Every detail string here is built from typed
// error fields and identifiers, never from wrapped server output; fields
// that embed database-sourced identifiers are sanitized before they leave,
// so a detail is safe to render on operator-facing surfaces.
func classifyRefusal(err error, table string) *refusal {
	var privilegeErr *preflight.PrivilegeError
	if errors.As(err, &privilegeErr) {
		detail := fmt.Sprintf("the engine role lacks access for %s on table %q; provision with: %s (verified by: %s)",
			privilegeErr.Tier, table, privilegeErr.Grant, privilegeErr.Check)
		if privilegeErr.Hint != "" {
			detail += "; " + privilegeErr.Hint
		}
		return &refusal{reason: "insufficient-privileges", detail: sanitizeReasonText(detail)}
	}
	var budgetErr *executor.BudgetError
	if errors.As(err, &budgetErr) && budgetErr.Cause == executor.CauseStatement {
		return &refusal{reason: "not-native-safe-budget-exceeded", detail: budgetErr.Error()}
	}
	var sizeErr *preflight.SizeError
	if errors.As(err, &sizeErr) {
		// Name whose limit this is: read cold, the size text sounds like a
		// property of PostgreSQL or of the change, when it is SchemaBot's
		// own conservatism for the native-safe path.
		return &refusal{reason: "table-too-large",
			detail: sizeErr.Error() + "; this threshold is SchemaBot's ceiling for a native-safe apply, not a PostgreSQL limit"}
	}
	if errors.Is(err, preflight.ErrTableNotFound) {
		return &refusal{reason: "table-not-found",
			detail: fmt.Sprintf("table %q does not exist on the target; re-plan against the current schema", table)}
	}
	if errors.Is(err, preflight.ErrNotTable) {
		return &refusal{reason: "not-a-table",
			detail: fmt.Sprintf("%q exists but is not an ordinary or partitioned table", table)}
	}
	return nil
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

	statement, err := pgstatement.ParseOne(change.sql)
	if err != nil {
		return fmt.Errorf("parse planned PostgreSQL statement for table %q: %w", change.table, err)
	}
	tier, err := preflight.RequiredTier([]string{change.sql})
	if err != nil {
		return fmt.Errorf("derive privilege tier for table %q: %w", change.table, err)
	}
	if _, err := preflight.CheckPrivileges(ctx, pool, change.namespace, change.table, preflight.Requirement{Tier: tier}); err != nil {
		return fmt.Errorf("check privileges for PostgreSQL table %q: %w", change.table, err)
	}
	table, err := preflight.CheckTable(ctx, pool, change.namespace, change.table, tableSizeLimit)
	if err != nil {
		return fmt.Errorf("preflight PostgreSQL table %q: %w", change.table, err)
	}
	if err := executor.ExecuteNative(ctx, pool, table, statement, executor.Budget{
		LockTimeout: optimisticLockTimeout, StatementTimeout: optimisticStatementLimit,
	}, executor.DefaultRetryPolicy()); err != nil {
		return fmt.Errorf("execute native-safe PostgreSQL statement on table %q: %w", change.table, err)
	}
	return nil
}

// Progress reports phase, elapsed time, and statement position for the apply
// the caller identifies via ResumeState.MigrationContext. A caller asking
// about an apply the engine is not tracking gets the idle sentinel: one
// engine is shared for the lifetime of a target, so answering with whichever
// apply wrote last would report another schema change's state — including a
// terminal one — for work that is still in flight. Rich server progress is
// intentionally absent until the PostgreSQL executor exposes it.
func (e *Engine) Progress(_ context.Context, req *engine.ProgressRequest) (*engine.ProgressResult, error) {
	var key string
	if req != nil {
		key = progressIdentity(req.ResumeState)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.progress == nil || key != e.progressKey {
		// The exact idle message is a cross-engine contract: stale-task
		// recovery compares against it verbatim to auto-resolve work
		// abandoned by a crashed server.
		return &engine.ProgressResult{State: engine.StatePending, Message: "No active schema change"}, nil
	}
	result := *e.progress
	result.Metadata = cloneMetadata(e.progress.Metadata)
	result.Tables = cloneTables(e.progress.Tables)
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
	result := &engine.ProgressResult{
		State: state, Progress: progress, Message: "PostgreSQL schema change " + phase,
		ErrorMessage: detail,
		Metadata: map[string]string{
			"phase": phase, "elapsed": time.Since(started).Round(time.Millisecond).String(),
			"step": "1", "steps_total": "1",
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

// claimProgress records an accepted apply as the engine's tracked schema
// change. Only Apply calls this: acceptance is the moment the engine's
// single progress slot changes hands.
func (e *Engine) claimProgress(key string, result *engine.ProgressResult) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.progressKey = key
	e.progress = result
}

// publishProgress stores a background apply's progress unless a newer apply
// has claimed the engine since. A stale writer must never overwrite the
// tracked apply's state, so the dropped write is logged and discarded — the
// superseded apply's poller reads the idle sentinel instead.
func (e *Engine) publishProgress(key string, result *engine.ProgressResult, logger *slog.Logger) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if key != e.progressKey {
		logger.Warn("PostgreSQL apply progress discarded: a newer apply claimed the engine",
			"task_id", key, "state", result.State, "tracked_task_id", e.progressKey)
		return
	}
	e.progress = result
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
