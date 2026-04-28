// Package state defines canonical state constants for SchemaBot's internal
// state machines (Apply, Task) and external engine states (Vitess, Spirit).
package state

import "strings"

// Apply holds the apply-level state machine constants.
// An apply is a single schema change operation stored in the applies table.
//
// The state machine is a union across all engines. Some states are only valid
// for specific engines (e.g., CreatingBranch and RevertWindow are PlanetScale-only,
// Stopped with resume is Spirit-only). Each engine uses the subset that applies
// to its lifecycle. Consumers (CLI, TUI, PR templates) handle all states via
// switch/case with a default fallback for unknown states.
var Apply = struct {
	Pending           string
	Running           string
	WaitingForCutover string
	CuttingOver       string
	RevertWindow      string
	Completed         string
	Failed            string
	FailedRetryable   string
	Stopped           string
	Cancelled         string
	Reverted          string

	// PlanetScale-specific states for the branch/deploy lifecycle.
	// These are set on the apply record during engine setup so the
	// progress handler and CLI can show what's happening.
	CreatingBranch        string
	ApplyingBranchChanges string
	CreatingDeployRequest string
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
	Cancelled:         "cancelled",
	Reverted:          "reverted",

	CreatingBranch:        "creating_branch",
	ApplyingBranchChanges: "applying_branch_changes",
	CreatingDeployRequest: "creating_deploy_request",
}

// DeriveApplyState determines the overall Apply state from individual Task states.
//
// State priority (highest to lowest):
//  1. Any task FAILED → Apply FAILED
//  2. Any task STOPPED → Apply STOPPED
//  3. Any task REVERTED → Apply REVERTED
//  4. All tasks COMPLETED → Apply COMPLETED
//  5. Any task CUTTING_OVER → Apply CUTTING_OVER
//  6. All non-completed tasks WAITING_FOR_CUTOVER → Apply WAITING_FOR_CUTOVER
//  7. Any task REVERT_WINDOW → Apply REVERT_WINDOW
//  8. Any task RUNNING → Apply RUNNING
//  9. Otherwise → Apply PENDING
//
// taskStates should be the State field from each Task. Empty slice returns PENDING.
func DeriveApplyState(taskStates []string) string {
	if len(taskStates) == 0 {
		return Apply.Pending
	}

	counts := make(map[string]int)
	for _, s := range taskStates {
		counts[normalizeApplyState(s)]++
	}

	total := len(taskStates)

	if counts[Apply.Failed] > 0 {
		return Apply.Failed
	}
	if counts[Apply.FailedRetryable] > 0 {
		return Apply.FailedRetryable
	}
	if counts[Apply.Cancelled] > 0 {
		return Apply.Cancelled
	}
	if counts[Apply.Stopped] > 0 {
		return Apply.Stopped
	}
	if counts[Apply.Reverted] > 0 {
		return Apply.Reverted
	}
	if counts[Apply.Completed] == total {
		return Apply.Completed
	}
	if counts[Apply.CuttingOver] > 0 {
		return Apply.CuttingOver
	}
	waitingOrCompleted := counts[Apply.WaitingForCutover] + counts[Apply.Completed]
	if waitingOrCompleted == total && counts[Apply.WaitingForCutover] > 0 {
		return Apply.WaitingForCutover
	}
	if counts[Apply.RevertWindow] > 0 {
		return Apply.RevertWindow
	}
	if counts[Apply.Running] > 0 {
		return Apply.Running
	}
	return Apply.Pending
}

// normalizeApplyState converts a task state string to its canonical lowercase form.
func normalizeApplyState(raw string) string {
	switch strings.ToUpper(raw) {
	case "PENDING":
		return Apply.Pending
	case "RUNNING":
		return Apply.Running
	case "WAITING_FOR_CUTOVER":
		return Apply.WaitingForCutover
	case "CUTTING_OVER":
		return Apply.CuttingOver
	case "REVERT_WINDOW":
		return Apply.RevertWindow
	case "COMPLETED", "COMPLETE":
		return Apply.Completed
	case "FAILED":
		return Apply.Failed
	case "FAILED_RETRYABLE":
		return Apply.FailedRetryable
	case "STOPPED":
		return Apply.Stopped
	case "CANCELLED":
		return Apply.Cancelled
	case "REVERTED":
		return Apply.Reverted
	default:
		return Apply.Pending
	}
}

// IsState checks if the given state matches any of the expected states.
// Strips the "STATE_" prefix used by protobuf enum names (e.g. ternv1.State_STATE_COMPLETED)
// so that proto, short ("COMPLETED"), and canonical lowercase ("completed") formats all match.
// Comparison is case-insensitive.
func IsState(s string, expected ...string) bool {
	norm := NormalizeState(s)
	for _, exp := range expected {
		if norm == NormalizeState(exp) {
			return true
		}
	}
	return false
}

// IsTerminalApplyState returns true if the state is a terminal state
// where no further processing will occur. FailedRetryable is NOT terminal
// — the recovery loop may re-dispatch the apply.
func IsTerminalApplyState(s string) bool {
	switch s {
	case Apply.Completed, Apply.Failed, Apply.Stopped, Apply.Cancelled, Apply.Reverted:
		return true
	default:
		return false
	}
}

// IsPlanetScaleEngine returns true if the engine string indicates PlanetScale/Vitess.
// Handles display names ("PlanetScale"), storage constants ("planetscale"),
// and proto enum strings ("ENGINE_PLANETSCALE").
func IsPlanetScaleEngine(engine string) bool {
	return strings.EqualFold(engine, "planetscale") || strings.EqualFold(engine, "ENGINE_PLANETSCALE")
}
