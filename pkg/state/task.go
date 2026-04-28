package state

import (
	"strings"

	spiritstatus "github.com/block/spirit/pkg/status"
	vitessstatus "vitess.io/vitess/go/vt/schema"
)

// Task holds the task-level state machine constants.
// A task is a per-table unit of work stored in the tasks table.
var Task = struct {
	Pending           string
	Running           string
	WaitingForCutover string
	CuttingOver       string
	RevertWindow      string
	Completed         string
	Failed            string
	FailedRetryable   string
	Stopped           string
	Reverted          string
	Cancelled         string
}{
	Pending:           "pending",
	Running:           "running",
	WaitingForCutover: "waiting_for_cutover",
	CuttingOver:       "cutting_over",
	RevertWindow:      "revert_window",
	Completed:         "completed",
	Failed:            "failed",
	FailedRetryable:   "failed_retryable",
	Stopped:           "stopped",
	Reverted:          "reverted",
	Cancelled:         "cancelled",
}

// TerminalTaskStates lists all task states where no further processing will occur.
// Stopped is intentionally excluded — a stopped task can be resumed via Start.
var TerminalTaskStates = []string{
	Task.Completed,
	Task.Failed,
	Task.Reverted,
	Task.Cancelled,
}

// IsTerminalTaskState returns true if the state is a terminal task state
// where no further processing will occur.
// Stopped is NOT terminal — a stopped task can be resumed via Start.
// FailedRetryable is NOT terminal — the recovery loop may retry the task.
func IsTerminalTaskState(s string) bool {
	switch s {
	case Task.Completed, Task.Failed, Task.Reverted, Task.Cancelled:
		return true
	default:
		return false
	}
}

// NormalizeTaskStatus maps a raw engine status to a canonical Task state.
// Called at the parsing boundary (ParseProgressResponse) so rendering code
// can compare against Task.* constants directly.
//
// Inputs arrive as exact engine strings: Spirit camelCase ("copyRows"),
// Vitess lowercase ("running"), or storage snake_case ("waiting_for_cutover").
func NormalizeTaskStatus(raw string) string {
	s := strings.TrimPrefix(strings.TrimPrefix(raw, "STATE_"), "state_")

	switch s {
	// Completed — Vitess "complete", Spirit "close"
	case string(vitessstatus.OnlineDDLStatusComplete),
		spiritstatus.Close.String():
		return Task.Completed

	// Running — Spirit sub-states (camelCase from Spirit's State.String())
	case spiritstatus.CopyRows.String(),
		spiritstatus.Initial.String(),
		spiritstatus.ApplyChangeset.String(),
		spiritstatus.RestoreSecondaryIndexes.String(),
		spiritstatus.AnalyzeTable.String(),
		spiritstatus.Checksum.String(),
		spiritstatus.PostChecksum.String(),
		spiritstatus.ErrCleanup.String():
		return Task.Running

	// Running — Vitess
	case string(vitessstatus.OnlineDDLStatusRunning):
		return Task.Running

	// Waiting for cutover
	case spiritstatus.WaitingOnSentinelTable.String(), "ready_to_complete":
		return Task.WaitingForCutover

	// Cutting over
	case spiritstatus.CutOver.String():
		return Task.CuttingOver

	// Pending — Vitess queue states
	case string(vitessstatus.OnlineDDLStatusQueued),
		string(vitessstatus.OnlineDDLStatusReady),
		string(vitessstatus.OnlineDDLStatusRequested):
		return Task.Pending

	// Failed
	case string(vitessstatus.OnlineDDLStatusFailed):
		return Task.Failed

	// Cancelled
	case string(vitessstatus.OnlineDDLStatusCancelled):
		return Task.Cancelled

	// Pass-through for already-normalized values
	case Task.Pending, Task.Running, Task.Completed, Task.Stopped, Task.Failed,
		Task.FailedRetryable, Task.RevertWindow, Task.Reverted,
		Task.WaitingForCutover, Task.CuttingOver, Task.Cancelled:
		return s

	default:
		return Task.Running
	}
}

// NormalizeShardStatus maps a raw shard status to a canonical Task state.
func NormalizeShardStatus(raw string) string {
	return NormalizeTaskStatus(raw)
}
