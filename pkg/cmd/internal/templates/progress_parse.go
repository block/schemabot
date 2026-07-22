package templates

import (
	"math"
	"regexp"
	"strconv"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
)

// spiritProgressPattern matches the row-copy prefix of a Spirit progress
// string, e.g. "71436/221193 32.30% copyRows". The ETA is carried separately as
// a structured field, so it is not parsed out of this string.
var spiritProgressPattern = regexp.MustCompile(`(\d+)/(\d+)\s+([\d.]+)%\s+(\w+)`)

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
	IsInstant           bool
	ProgressDetail      string // e.g., Spirit: "12.5% copyRows ETA 1h 30m"
	Shards              []ShardProgress
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

// SpiritProgressInfo contains parsed Spirit progress information.
type SpiritProgressInfo struct {
	RowsCopied int64
	RowsTotal  int64
	Percent    int
	State      string // "copyRows", "checksum", etc.
}

// ParseSpiritProgress parses a Spirit progress string like "71436/221193 32.30% copyRows ETA TBD"
// Returns nil if the string cannot be parsed.
func ParseSpiritProgress(progress string) *SpiritProgressInfo {
	if progress == "" {
		return nil
	}

	matches := spiritProgressPattern.FindStringSubmatch(progress)
	if len(matches) < 5 {
		return nil
	}

	rowsCopied, _ := strconv.ParseInt(matches[1], 10, 64)
	rowsTotal, _ := strconv.ParseInt(matches[2], 10, 64)
	percentFloat, _ := strconv.ParseFloat(matches[3], 64)
	state := matches[4]

	return &SpiritProgressInfo{
		RowsCopied: rowsCopied,
		RowsTotal:  rowsTotal,
		Percent:    int(math.Round(percentFloat)),
		State:      state,
	}
}

// Display-only task states. These are not persisted apply states (see pkg/applystate)
// but are used for per-table rendering in sequential mode.
const (
	TaskCancelled = "cancelled" // Table was never executed due to earlier failure
)

// ParseProgressResponse converts a typed ProgressResponse to ProgressData for rendering.
func ParseProgressResponse(result *apitypes.ProgressResponse) ProgressData {
	data := ProgressData{
		ApplyID:      result.ApplyID,
		Database:     result.Database,
		Environment:  result.Environment,
		Caller:       result.Caller,
		State:        state.NormalizeState(result.State),
		Engine:       result.Engine,
		ErrorMessage: result.ErrorMessage,
		StartedAt:    result.StartedAt,
		CompletedAt:  result.CompletedAt,
		Options:      result.Options,
		Metadata:     result.Metadata,
		Released:     result.Released,
	}

	for _, op := range result.Operations {
		data.Operations = append(data.Operations, ProgressOperation{
			Deployment:          op.Deployment,
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

	for _, tbl := range result.Tables {
		tp := TableProgress{
			TableName:           tbl.TableName,
			Deployment:          tbl.Deployment,
			Namespace:           tbl.Keyspace,
			ChangeType:          tbl.ChangeType,
			DDL:                 tbl.DDL,
			Status:              state.NormalizeState(tbl.Status),
			RowsCopied:          tbl.RowsCopied,
			RowsTotal:           tbl.RowsTotal,
			PercentComplete:     int(tbl.PercentComplete),
			ETASeconds:          tbl.ETASeconds,
			ChecksumRowsChecked: tbl.ChecksumRowsChecked,
			ChecksumRowsTotal:   tbl.ChecksumRowsTotal,
			IsInstant:           tbl.IsInstant,
			ProgressDetail:      tbl.ProgressDetail,
		}
		for _, sh := range tbl.Shards {
			tp.Shards = append(tp.Shards, ShardProgress{
				Shard:           sh.Shard,
				Status:          state.NormalizeShardStatus(sh.Status),
				RowsCopied:      sh.RowsCopied,
				RowsTotal:       sh.RowsTotal,
				ETASeconds:      sh.ETASeconds,
				PercentComplete: int(sh.PercentComplete),
				CutoverAttempts: int(sh.CutoverAttempts),
			})
		}
		data.Tables = append(data.Tables, tp)
	}

	return data
}
