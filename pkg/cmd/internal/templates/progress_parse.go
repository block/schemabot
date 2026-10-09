package templates

import (
	"log/slog"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/ui"
)

// ProgressData contains data for rendering schema change progress.
type ProgressData struct {
	ApplyID        string
	Database       string
	Environment    string
	Caller         string
	PullRequestURL string
	State          string
	Engine         string
	ErrorMessage   string
	StartedAt      string // RFC3339 format
	CompletedAt    string // RFC3339 format
	Step           int
	StepsTotal     int
	Statement      string
	BuildWork      apitypes.BuildWork
	Operations     []ProgressOperation
	Tables         []TableProgress
	Options        map[string]string // Apply options (defer_cutover, skip_revert, etc.)
	Metadata       map[string]string // Engine metadata (e.g., deploy_request_url, branch_name)
	// Released is true when an operator has released a paused rollout open, so a
	// deployment that failed under on_failure=pause no longer holds later
	// deployments. Apply-level: it applies to every operation of the apply.
	Released bool
}

// ProgressOperation represents progress for one deployment operation.
type ProgressOperation struct {
	Deployment          string
	OperationKey        string
	ExternalID          string
	ExternalOperationID string
	OperationKind       string
	Target              string
	State               string
	CutoverPolicy       string
	OnFailure           string
	ErrorMessage        string
	ErrorCode           string
	AlreadyConverged    bool
	// RolloutStep is the table step the operation runs when the rollout runs
	// table by table, and 0 otherwise.
	RolloutStep int
	StartedAt   string
	CompletedAt string
}

// TableProgress represents progress for a single table schema change.
type TableProgress struct {
	TableName  string
	Deployment string
	// Target is the address within Deployment this table's copy ran against. One
	// deployment can address several targets, each copying the same table
	// separately, so a section scoped to a deployment alone would merge them.
	Target          string
	Namespace       string // Keyspace (Vitess) or schema name (MySQL)
	Dialect         schema.Dialect
	ChangeType      string // create, alter, drop
	DDL             string
	Status          string
	RowsCopied      int64
	RowsTotal       int64
	PercentComplete int
	ETASeconds      int64
	// EstimatedBytes is the table's on-disk size when it was planned, shown
	// beside the row counts. Nil when the plan had no estimate.
	EstimatedBytes *int64
	// Checksum phase progress: rows verified so far and total to verify.
	// Non-zero only while the table is checksumming (verifying copied data).
	ChecksumRowsChecked int64
	ChecksumRowsTotal   int64
	// The engine's throttler is pausing this table's active phase (row copy
	// or checksum verify). ThrottleReason names the signal for display and is
	// empty when Throttled is false.
	Throttled      bool
	ThrottleReason string
	IsInstant      bool
	// Shards is the table's per-part progress: one entry per shard, or one per
	// target when AcrossTargets is set.
	Shards []ShardProgress
	// AcrossTargets marks a table that stands for one change across a
	// rollout's targets, rolled up the way a sharded table rolls up its shards.
	AcrossTargets bool
	// OnTargets names the targets a rolled-up table's DDL runs on when they
	// are only some of the deployment's, shown above the DDL. Empty when the
	// DDL runs on every target.
	OnTargets string
	// UnreportedTargets counts the targets a rolled-up table speaks for that
	// are still to run and have reported no progress yet, so have not started
	// it. A settled target is not among them.
	UnreportedTargets int
}

// ShardProgress contains per-shard progress for template rendering.
type ShardProgress struct {
	Shard           string
	Status          string
	RowsCopied      int64
	RowsTotal       int64
	ETASeconds      int64
	PercentComplete int
	CutoverAttempts int
}

// Display-only task states. These are not persisted apply states (see pkg/applystate)
// but are used for per-table rendering in sequential mode.
const (
	TaskCancelled = "cancelled" // Table was never executed due to earlier failure
)

// ParseProgressResponse converts a typed ProgressResponse to ProgressData for rendering.
func ParseProgressResponse(result *apitypes.ProgressResponse) ProgressData {
	data := ProgressData{
		ApplyID:        result.ApplyID,
		Database:       result.Database,
		Environment:    result.Environment,
		Caller:         result.Caller,
		PullRequestURL: result.PullRequest,
		State:          state.NormalizeState(result.State),
		Engine:         result.Engine,
		ErrorMessage:   result.ErrorMessage,
		StartedAt:      result.StartedAt,
		CompletedAt:    result.CompletedAt,
		Options:        result.Options,
		Metadata:       result.Metadata,
		Released:       result.Released,
	}
	if step, err := apitypes.ParseProgressStep(result.Metadata); err != nil {
		slog.Warn("progress output omits the statement position because the progress metadata is malformed", "apply_id", result.ApplyID, "error", err)
	} else {
		data.Step, data.StepsTotal, data.Statement = step.Step, step.StepsTotal, step.Statement
	}
	if work, err := apitypes.ParseBuildWork(result.Metadata); err != nil {
		slog.Warn("progress output omits build work because the progress metadata is malformed", "apply_id", result.ApplyID, "error", err)
	} else {
		data.BuildWork = work
	}
	dialect := schema.DialectForDatabaseType(result.DatabaseType)

	for _, op := range result.Operations {
		data.Operations = append(data.Operations, ProgressOperation{
			Deployment:          op.Deployment,
			OperationKey:        op.OperationKey,
			ExternalID:          op.ExternalID,
			ExternalOperationID: op.ExternalOperationID,
			OperationKind:       op.OperationKind,
			Target:              op.Target,
			State:               state.NormalizeState(op.State),
			CutoverPolicy:       op.CutoverPolicy,
			OnFailure:           op.OnFailure,
			ErrorMessage:        op.ErrorMessage,
			ErrorCode:           op.ErrorCode,
			AlreadyConverged:    op.AlreadyConverged,
			RolloutStep:         op.RolloutStep,
			StartedAt:           op.StartedAt,
			CompletedAt:         op.CompletedAt,
		})
	}

	for _, tbl := range ddl.FilterInternalTablesTyped(result.Tables) {
		tp := TableProgress{
			TableName:           tbl.TableName,
			Deployment:          tbl.Deployment,
			Target:              tbl.Target,
			Namespace:           tbl.Keyspace,
			Dialect:             dialect,
			ChangeType:          tbl.ChangeType,
			DDL:                 tbl.DDL,
			Status:              state.NormalizeTaskStatus(tbl.Status),
			RowsCopied:          tbl.RowsCopied,
			RowsTotal:           tbl.RowsTotal,
			EstimatedBytes:      tbl.EstimatedBytes,
			PercentComplete:     int(tbl.PercentComplete),
			ETASeconds:          tbl.ETASeconds,
			ChecksumRowsChecked: tbl.ChecksumRowsChecked,
			ChecksumRowsTotal:   tbl.ChecksumRowsTotal,
			Throttled:           tbl.Throttled,
			ThrottleReason:      tbl.ThrottleReason,
			IsInstant:           tbl.IsInstant,
		}
		for _, sh := range tbl.Shards {
			pct := int(sh.PercentComplete)
			if pct == 0 && sh.RowsTotal > 0 {
				pct = int(sh.RowsCopied * 100 / sh.RowsTotal)
			}
			// Row totals are estimates, so a nearly finished copy can exceed
			// them whether the percent arrived from the server or was derived
			// above; clamp so the rendered percent stays honest.
			pct = ui.ClampPercent(pct)
			tp.Shards = append(tp.Shards, ShardProgress{
				Shard:           sh.Shard,
				Status:          state.NormalizeShardStatus(sh.Status),
				RowsCopied:      sh.RowsCopied,
				RowsTotal:       sh.RowsTotal,
				ETASeconds:      sh.ETASeconds,
				PercentComplete: pct,
				CutoverAttempts: int(sh.CutoverAttempts),
			})
			// Table-level ETA: the table's own estimate (MySQL/Spirit), or
			// the slowest shard's for a sharded (Vitess) table.
			if sh.ETASeconds > tp.ETASeconds {
				tp.ETASeconds = sh.ETASeconds
			}
		}
		data.Tables = append(data.Tables, tp)
	}

	return data
}
