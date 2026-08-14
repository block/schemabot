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
	"github.com/block/schemabot/pkg/postgresconn"
)

const (
	optimisticTableSizeLimit = int64(1 << 30)
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

// Apply starts one native-safe PostgreSQL statement under pg-sprite's bounded
// optimistic executor. Planner-produced sequences use the same execution seam
// but are deliberately not admitted by this first increment.
func (e *Engine) Apply(ctx context.Context, req *engine.ApplyRequest) (*engine.ApplyResult, error) {
	change, err := validateOptimisticApply(req)
	if err != nil {
		return nil, err
	}

	started := time.Now()
	e.setProgress(progressResult(engine.StateRunning, "preflight", started, change, ""))
	bgCtx := context.WithoutCancel(ctx)
	dsn := req.Credentials.DSN
	logger := req.Logger
	if logger == nil {
		logger = slog.Default()
	}
	e.wg.Go(func() { e.runOptimisticApply(bgCtx, dsn, change, started, logger) })

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
	return nativeApply{namespace: req.Changes[0].Namespace, table: tc.Table, sql: tc.DDL}, nil
}

func (e *Engine) runOptimisticApply(ctx context.Context, dsn string, change nativeApply, started time.Time, logger *slog.Logger) {
	// The context arrives detached from the caller (an accepted apply must
	// survive the request), so boundedness comes from the ceiling instead.
	ctx, cancel := context.WithTimeout(ctx, optimisticApplyCeiling)
	defer cancel()
	err := executeOptimistic(ctx, dsn, change)
	if err == nil {
		e.setProgress(progressResult(engine.StateCompleted, "completed", started, change, ""))
		return
	}

	var privilegeErr *preflight.PrivilegeError
	if errors.As(err, &privilegeErr) {
		detail := fmt.Sprintf("insufficient privileges; provision with: %s", privilegeErr.Grant)
		if privilegeErr.Hint != "" {
			detail += "; " + privilegeErr.Hint
		}
		result := progressResult(engine.StateFailed, "refused", started, change, detail)
		result.Metadata["execution_mode"] = engine.ExecutionModeBlocked
		result.Metadata["refusal_reason"] = "insufficient-privileges"
		e.setProgress(result)
		return
	}

	var budgetErr *executor.BudgetError
	if errors.As(err, &budgetErr) {
		result := progressResult(engine.StateFailed, "refused", started, change, budgetErr.Error())
		result.Metadata["execution_mode"] = engine.ExecutionModeBlocked
		result.Metadata["refusal_reason"] = "not-native-safe-budget-exceeded"
		e.setProgress(result)
		return
	}

	logger.Error("PostgreSQL native-safe schema change failed", "namespace", change.namespace, "table", change.table, "error", err)
	e.setProgress(progressResult(engine.StateFailed, "failed", started, change, "PostgreSQL schema change failed; see server logs"))
}

func executeOptimistic(ctx context.Context, rawDSN string, change nativeApply) error {
	dsn, err := postgresconn.ConnectionDSN(rawDSN)
	if err != nil {
		return fmt.Errorf("normalize PostgreSQL DSN for apply: %w", err)
	}
	pool, err := dbconn.NewPool(ctx, dbconn.Config{URL: dsn})
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
	table, err := preflight.CheckTable(ctx, pool, change.namespace, change.table, optimisticTableSizeLimit)
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

// Progress reports phase, elapsed time, and statement position. Rich server
// progress is intentionally absent until the PostgreSQL executor exposes it.
func (e *Engine) Progress(_ context.Context, _ *engine.ProgressRequest) (*engine.ProgressResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.progress == nil {
		return &engine.ProgressResult{State: engine.StatePending, Message: "No active PostgreSQL schema change"}, nil
	}
	result := *e.progress
	result.Metadata = cloneMetadata(e.progress.Metadata)
	result.Tables = cloneTables(e.progress.Tables)
	if len(result.Tables) > 0 && result.Tables[0].StartedAt != nil && !result.State.IsTerminal() {
		result.Metadata["elapsed"] = time.Since(*result.Tables[0].StartedAt).Round(time.Millisecond).String()
	}
	return &result, nil
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

func (e *Engine) setProgress(result *engine.ProgressResult) {
	e.mu.Lock()
	defer e.mu.Unlock()
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
