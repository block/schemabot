// Package state defines canonical state constants for SchemaBot's internal
// state machines (Apply, Task) and external engine states (Vitess, Spirit).
package state

import "strings"

// Apply holds the apply-level state machine constants.
// An apply is a single schema change operation stored in the applies table.
var Apply = struct {
	Pending           string
	Running           string
	WaitingForCutover string
	CuttingOver       string
	RevertWindow      string
	Completed         string
	Failed            string
	Stopped           string
	Reverted          string
}{
	Pending:           "pending",
	Running:           "running",
	WaitingForCutover: "waiting_for_cutover",
	CuttingOver:       "cutting_over",
	RevertWindow:      "revert_window",
	Completed:         "completed",
	Failed:            "failed",
	Stopped:           "stopped",
	Reverted:          "reverted",
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
	case "STOPPED":
		return Apply.Stopped
	case "REVERTED":
		return Apply.Reverted
	case "CANCELLED":
		return Apply.Failed
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
// where no further processing will occur.
func IsTerminalApplyState(s string) bool {
	switch s {
	case Apply.Completed, Apply.Failed, Apply.Stopped, Apply.Reverted:
		return true
	default:
		return false
	}
}
