// Package direct holds the engine-agnostic core of direct execution: the
// per-database policy parsed from engine metadata and the resolver that
// decides whether a statement the engine refuses may run directly on the
// target. An engine that adopts direct execution owns its refusal detector,
// its size estimator, and its executor; the policy schema and the verdict
// resolution here are shared so every engine applies the same fail-closed
// semantics and reports the same outcome vocabulary.
package direct

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/ui"
)

// Policy is the resolved direct execution policy for a target database.
// The zero value is the fail-closed default: refused statements are blocked.
type Policy struct {
	Enabled      bool
	MaxTableRows int64
	// LockAcquisitionTimeoutSeconds bounds each direct statement's lock
	// acquisition. Zero means the policy did not set one; read the effective
	// value through EffectiveLockAcquisitionTimeoutSeconds, which applies the
	// engine's default.
	LockAcquisitionTimeoutSeconds int64
}

// EffectiveLockAcquisitionTimeoutSeconds returns the lock-acquisition bound
// for direct statements: the policy's configured value, or the caller's
// engine default when the policy leaves it unset. The default is per-engine
// because each engine bounds lock acquisition with its own mechanism.
func (p Policy) EffectiveLockAcquisitionTimeoutSeconds(engineDefault int64) int64 {
	if p.LockAcquisitionTimeoutSeconds > 0 {
		return p.LockAcquisitionTimeoutSeconds
	}
	return engineDefault
}

// PolicyFromMetadata parses the direct execution policy from engine
// metadata. Malformed values are errors rather than a silent fallback to
// disabled, so a misconfigured policy is surfaced instead of quietly turning
// planned direct changes into apply-time failures.
func PolicyFromMetadata(md map[string]string) (Policy, error) {
	rawEnabled := md[engine.MetadataDirectExecution]
	if rawEnabled == "" {
		return Policy{}, nil
	}
	enabled, err := strconv.ParseBool(rawEnabled)
	if err != nil {
		return Policy{}, fmt.Errorf("invalid %s metadata value %q: %w", engine.MetadataDirectExecution, rawEnabled, err)
	}
	if !enabled {
		return Policy{}, nil
	}
	raw := md[engine.MetadataDirectExecutionMaxTableRows]
	if raw == "" {
		return Policy{}, fmt.Errorf("%s is enabled but %s is not set: the row bound is required so direct execution fails closed on large tables", engine.MetadataDirectExecution, engine.MetadataDirectExecutionMaxTableRows)
	}
	maxRows, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return Policy{}, fmt.Errorf("parse %s metadata value %q: %w", engine.MetadataDirectExecutionMaxTableRows, raw, err)
	}
	if maxRows <= 0 {
		return Policy{}, fmt.Errorf("%s must be positive, got %d", engine.MetadataDirectExecutionMaxTableRows, maxRows)
	}
	policy := Policy{Enabled: true, MaxTableRows: maxRows}
	if raw := md[engine.MetadataDirectExecutionLockAcquisitionTimeoutSeconds]; raw != "" {
		lockWait, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return Policy{}, fmt.Errorf("parse %s metadata value %q: %w", engine.MetadataDirectExecutionLockAcquisitionTimeoutSeconds, raw, err)
		}
		if lockWait <= 0 {
			return Policy{}, fmt.Errorf("%s must be positive, got %d", engine.MetadataDirectExecutionLockAcquisitionTimeoutSeconds, lockWait)
		}
		policy.LockAcquisitionTimeoutSeconds = lockWait
	}
	return policy, nil
}

// Metric outcome labels for decisions that block a refused statement. The
// resolver records the outcome on the Decision; callers feed it to the
// direct-execution counter so every engine reports blocked verdicts under
// one vocabulary.
const (
	// OutcomeBlockedPolicyDisabled marks a refused statement blocked because
	// the database has no enabled direct execution policy.
	OutcomeBlockedPolicyDisabled = "blocked_policy_disabled"
	// OutcomeBlockedSizeUnknown marks a refused statement blocked because the
	// size gate could not be evaluated: no target connection, no estimate, or
	// a failed count. Unknown size blocks, never assumes small.
	OutcomeBlockedSizeUnknown = "blocked_size_unknown"
	// OutcomeBlockedSizeLimit marks a refused statement blocked because the
	// table measured above the policy's row bound.
	OutcomeBlockedSizeLimit = "blocked_size_limit"
)

// SizeEstimator measures a table for the resolver's two-step size gate. An
// engine implements it against its target database.
//
// Contract:
//   - EstimatedRows returns the engine's cheap statistics-based row estimate
//     (MySQL: information_schema TABLE_ROWS with statistics caching disabled;
//     PostgreSQL: the catalog's row estimate). An unknown or negative
//     estimate must be returned as an error, never as a count — negative
//     values are "no real estimate" sentinels (PostgreSQL's
//     pg_class.reltuples reports -1 for a never-analyzed table), and the
//     resolver blocks on any estimator error.
//   - ExactRowsWithin returns the table's exact row count with the scan
//     capped at limit+1, so confirming a table within the policy bound stays
//     cheap no matter how wrong the estimate is; a return of limit+1 means
//     "more than limit rows", not a total.
//
// Both methods must wrap their errors with enough context (connection vs
// query failure, table identifiers) for the resolver's blocked-verdict log
// to be triaged on its own.
type SizeEstimator interface {
	EstimatedRows(ctx context.Context, table string) (int64, error)
	ExactRowsWithin(ctx context.Context, table string, limit int64) (int64, error)
}

// Decision is how a statement the engine refuses will execute under the
// direct execution policy. The plan records Mode and ModeReason on the table
// change as an operator-facing preview; apply-time routing re-resolves the
// decision against live policy and table state rather than trusting the
// stored verdict.
type Decision struct {
	Mode       string // engine.ExecutionModeDirect or engine.ExecutionModeBlocked
	ModeReason string // operator-facing reason, including row-count context
	Outcome    string // metric outcome label when the decision blocks
	Rows       int64  // measured rows when the size gate ran: exact for a direct verdict, the estimate when the estimate alone blocked
}

// blockedSizeUnknownReason is the mode-reason suffix when the size gate could
// not be evaluated at all: no target connection, no statistics row, or a
// failed count. Every such uncertainty blocks.
const blockedSizeUnknownReason = "; direct execution is enabled but the table's row count is unavailable"

// Resolver decides whether a refused statement may run directly. The zero
// value is unusable; every field is required.
type Resolver struct {
	Logger *slog.Logger
	// Estimator measures the target's tables for the size gate.
	Estimator SizeEstimator
	// RunsAs names how a direct statement executes on this engine (e.g.
	// "native MySQL DDL"), rendered in the direct verdict's mode reason so
	// the operator-facing preview stays engine-accurate.
	RunsAs string
}

// ResolveRefusedMode decides whether the policy routes a refused statement to
// direct execution. Every uncertainty blocks: policy disabled, a size gate
// that cannot be evaluated, or a table above the configured bound. The size
// gate runs in two steps — the estimator's statistics-based estimate first,
// then an exact bounded row count — because the estimate can lag reality in
// both directions, so it is trusted to block but never to approve on its own.
// The above-bound reason names only the configured limit, not the measured
// count, so identical verdicts on different shards collapse into one row in
// PR-facing summaries.
//
// A non-nil error means the caller's context was cancelled while the size gate
// ran: the gate never finished, so there is no verdict to record. Reporting a
// cancellation as a blocked verdict would misdiagnose an operator stop as an
// unmeasurable table, so the two are kept distinct.
func (r Resolver) ResolveRefusedMode(ctx context.Context, policy Policy, database, tableName, refusalReason string) (Decision, error) {
	if !policy.Enabled {
		return Decision{
			Mode:       engine.ExecutionModeBlocked,
			ModeReason: refusalReason,
			Outcome:    OutcomeBlockedPolicyDisabled,
		}, nil
	}
	rows, err := r.Estimator.EstimatedRows(ctx, tableName)
	if err != nil {
		if ctx.Err() != nil {
			return Decision{}, fmt.Errorf("size gate for table %q interrupted during the row estimate: %w", tableName, err)
		}
		// Fail closed: a table whose size cannot be measured must never
		// rebuild natively — block it and surface why in the mode reason.
		r.Logger.Warn("direct execution blocked: estimated row count unavailable",
			"database", database, "table", tableName, "error", err)
		return Decision{
			Mode:       engine.ExecutionModeBlocked,
			ModeReason: refusalReason + blockedSizeUnknownReason,
			Outcome:    OutcomeBlockedSizeUnknown,
		}, nil
	}
	aboveBoundReason := fmt.Sprintf("%s; direct execution is enabled but the table is above the configured limit of %s rows",
		refusalReason, ui.FormatNumber(policy.MaxTableRows))
	if rows > policy.MaxTableRows {
		r.Logger.Info("direct execution blocked: estimated row count above the policy bound",
			"database", database, "table", tableName, "estimated_rows", rows, "max_table_rows", policy.MaxTableRows)
		return Decision{
			Mode:       engine.ExecutionModeBlocked,
			ModeReason: aboveBoundReason,
			Outcome:    OutcomeBlockedSizeLimit,
			Rows:       rows,
		}, nil
	}
	count, err := r.Estimator.ExactRowsWithin(ctx, tableName, policy.MaxTableRows)
	if err != nil {
		if ctx.Err() != nil {
			return Decision{}, fmt.Errorf("size gate for table %q interrupted during the exact row count: %w", tableName, err)
		}
		r.Logger.Warn("direct execution blocked: exact row count unavailable",
			"database", database, "table", tableName, "error", err)
		return Decision{
			Mode:       engine.ExecutionModeBlocked,
			ModeReason: refusalReason + blockedSizeUnknownReason,
			Outcome:    OutcomeBlockedSizeUnknown,
		}, nil
	}
	if count > policy.MaxTableRows {
		r.Logger.Info("direct execution blocked: exact row count above the policy bound despite a smaller estimate",
			"database", database, "table", tableName, "estimated_rows", rows, "max_table_rows", policy.MaxTableRows)
		return Decision{
			Mode:       engine.ExecutionModeBlocked,
			ModeReason: aboveBoundReason,
			Outcome:    OutcomeBlockedSizeLimit,
		}, nil
	}
	return Decision{
		Mode: engine.ExecutionModeDirect,
		ModeReason: fmt.Sprintf("%s; runs as %s on a table with ~%s rows",
			refusalReason, r.RunsAs, ui.FormatNumber(count)),
		Rows: count,
	}, nil
}
