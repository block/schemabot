package templates

import (
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
	Target              string
	State               string
	CutoverPolicy       string
	OnFailure           string
	ErrorMessage        string
	ErrorCode           string
	StartedAt           string
	CompletedAt         string
}

// TableProgress represents progress for a single table schema change.
type TableProgress struct {
	TableName       string
	Deployment      string
	Namespace       string // Keyspace (Vitess) or schema name (MySQL)
	Dialect         schema.Dialect
	ChangeType      string // create, alter, drop
	DDL             string
	Status          string
	RowsCopied      int64
	RowsTotal       int64
	PercentComplete int
	ETASeconds      int64
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
	Shards         []ShardProgress
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

// ShardCounts holds aggregated shard status counts.
type ShardCounts struct {
	Total             int
	Complete          int
	Running           int
	WaitingForCutover int
	CuttingOver       int
	Queued            int
	Failed            int
	Cancelled         int
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
	dialect := schema.DialectForDatabaseType(result.DatabaseType)

	for _, op := range result.Operations {
		data.Operations = append(data.Operations, ProgressOperation{
			Deployment:          op.Deployment,
			OperationKey:        op.OperationKey,
			ExternalID:          op.ExternalID,
			ExternalOperationID: op.ExternalOperationID,
			Target:              op.Target,
			State:               state.NormalizeState(op.State),
			CutoverPolicy:       op.CutoverPolicy,
			OnFailure:           op.OnFailure,
			ErrorMessage:        op.ErrorMessage,
			ErrorCode:           op.ErrorCode,
			StartedAt:           op.StartedAt,
			CompletedAt:         op.CompletedAt,
		})
	}

	for _, tbl := range ddl.FilterInternalTablesTyped(result.Tables) {
		tp := TableProgress{
			TableName:           tbl.TableName,
			Deployment:          tbl.Deployment,
			Namespace:           tbl.Keyspace,
			Dialect:             dialect,
			ChangeType:          tbl.ChangeType,
			DDL:                 tbl.DDL,
			Status:              state.NormalizeTaskStatus(tbl.Status),
			RowsCopied:          tbl.RowsCopied,
			RowsTotal:           tbl.RowsTotal,
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
