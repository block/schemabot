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
	CatchingUp        string
	Checksumming      string
	PostChecksum      string
	WaitingForDeploy  string
	WaitingForCutover string
	Recovering        string
	CuttingOver       string
	RevertWindow      string
	Reverting         string
	Completed         string
	Failed            string
	FailedRetryable   string
	Stopped           string
	Reverted          string
	Cancelled         string
}{
	Pending:           "pending",
	Running:           "running",
	CatchingUp:        "catching_up",
	Checksumming:      "checksumming",
	PostChecksum:      "post_checksum",
	WaitingForDeploy:  "waiting_for_deploy",
	WaitingForCutover: "waiting_for_cutover",
	Recovering:        "recovering",
	CuttingOver:       "cutting_over",
	RevertWindow:      "revert_window",
	Reverting:         "reverting",
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
// FailedRetryable is NOT terminal — operator drivers may retry the task.
func IsTerminalTaskState(s string) bool {
	switch s {
	case Task.Completed, Task.Failed, Task.Reverted, Task.Cancelled:
		return true
	default:
		return false
	}
}

// IsInFlightTaskState returns true if the state implies the engine is actively
// working on the task. These are the only states for which missing engine-side
// work means the task was abandoned (e.g. a server crash). Resting states such
// as Stopped and FailedRetryable also have no active engine work, but that
// absence is expected — Spirit keeps the checkpoint while the operator decides
// whether to resume or retry — so they are excluded here.
func IsInFlightTaskState(s string) bool {
	switch NormalizeState(s) {
	case Task.Running, Task.CatchingUp, Task.Checksumming, Task.PostChecksum, Task.WaitingForCutover, Task.CuttingOver, Task.WaitingForDeploy, Task.Recovering, Task.Reverting:
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
//
// Unknown statuses normalize to Running so unrecognized in-flight work stays
// visible and blocking; callers that need positive evidence of a state (rather
// than that fail-open default) must check RecognizedTaskStatus first.
func NormalizeTaskStatus(raw string) string {
	normalized, _ := normalizeTaskStatus(raw)
	return normalized
}

// RecognizedTaskStatus reports whether the raw engine status maps to a known
// task state, as opposed to falling back to NormalizeTaskStatus's in-flight
// default for unknown values.
func RecognizedTaskStatus(raw string) bool {
	_, recognized := normalizeTaskStatus(raw)
	return recognized
}

func normalizeTaskStatus(raw string) (string, bool) {
	s := strings.TrimPrefix(strings.TrimPrefix(raw, "STATE_"), "state_")

	switch s {
	// Completed — Vitess "complete", Spirit "close"
	case string(vitessstatus.OnlineDDLStatusComplete),
		spiritstatus.Close.String():
		return Task.Completed, true

	// Checksumming — Spirit verifies the copied data against the source before
	// cutover. On a large table this phase can run for hours, so it is surfaced
	// as its own table state rather than folded into Running.
	case spiritstatus.Checksum.String():
		return Task.Checksumming, true

	// Catching up — the row copy is done and Spirit is applying the changeset
	// accumulated from the binlog during the copy. This catch-up can run for
	// hours on a busy source and can even diverge, so it is surfaced as its
	// own table state rather than folded into Running, where a stalled
	// catch-up would render as a serene complete copy.
	case spiritstatus.ApplyChangeset.String():
		return Task.CatchingUp, true

	// Post-checksum — the verify passed and Spirit is applying the changes
	// that accumulated while it ran. Same catch-up mechanics as CatchingUp,
	// but it is a distinct, later phase: mapping it back to CatchingUp would
	// pin the stored (monotonic) task state at Checksumming for the whole
	// second drain, rendering an indeterminate verify that already finished.
	case spiritstatus.PostChecksum.String():
		return Task.PostChecksum, true

	// Running — Spirit sub-states (camelCase from Spirit's State.String())
	case spiritstatus.CopyRows.String(),
		spiritstatus.Initial.String(),
		spiritstatus.RestoreSecondaryIndexes.String(),
		spiritstatus.AnalyzeTable.String(),
		spiritstatus.ErrCleanup.String():
		return Task.Running, true

	// Running — Vitess
	case string(vitessstatus.OnlineDDLStatusRunning):
		return Task.Running, true

	// Waiting for cutover
	case spiritstatus.WaitingOnSentinelTable.String(), "ready_to_complete":
		return Task.WaitingForCutover, true

	// Cutting over
	case spiritstatus.CutOver.String():
		return Task.CuttingOver, true

	// Pending — Vitess queue states
	case string(vitessstatus.OnlineDDLStatusQueued),
		string(vitessstatus.OnlineDDLStatusReady),
		string(vitessstatus.OnlineDDLStatusRequested):
		return Task.Pending, true

	// Failed
	case string(vitessstatus.OnlineDDLStatusFailed):
		return Task.Failed, true

	// Cancelled
	case string(vitessstatus.OnlineDDLStatusCancelled):
		return Task.Cancelled, true

	// Pass-through for already-normalized values
	case Task.Pending, Task.Running, Task.CatchingUp, Task.Checksumming, Task.PostChecksum, Task.Completed, Task.Stopped, Task.Failed,
		Task.FailedRetryable, Task.RevertWindow, Task.Reverting, Task.Reverted,
		Task.WaitingForDeploy, Task.WaitingForCutover, Task.Recovering,
		Task.CuttingOver, Task.Cancelled:
		return s, true
	}

	switch normalized := NormalizeState(s); normalized {
	case NormalizeState(string(vitessstatus.OnlineDDLStatusComplete)):
		return Task.Completed, true
	case Task.Pending, Task.Running, Task.CatchingUp, Task.Checksumming, Task.PostChecksum, Task.Completed, Task.Stopped, Task.Failed,
		Task.FailedRetryable, Task.RevertWindow, Task.Reverting, Task.Reverted,
		Task.WaitingForDeploy, Task.WaitingForCutover, Task.Recovering,
		Task.CuttingOver, Task.Cancelled:
		return normalized, true
	default:
		// Unknown engine states represent in-flight work until proven otherwise.
		// Keep them visible and blocking, and add an explicit mapping once known.
		return Task.Running, false
	}
}

// NormalizeShardStatus maps a raw shard status to a canonical Task state.
func NormalizeShardStatus(raw string) string {
	return NormalizeTaskStatus(raw)
}
