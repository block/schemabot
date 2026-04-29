// Package state defines canonical state constants for SchemaBot's internal
// state machines (Apply, Task) and external engine states (Vitess, Spirit).
package state

import "strings"

// Apply holds the apply-level state machine constants.
// An apply is a single schema change operation stored in the applies table.
//
// The state machine is a union across all engines. Some states are only valid
// for specific engines (e.g., PreparingBranch and RevertWindow are PlanetScale-only,
// Stopped with resume is Spirit-only). Each engine uses the subset that applies
// to its lifecycle. Consumers (CLI, TUI, PR templates) handle all states via
// switch/case with a default fallback for unknown states.
var Apply = struct {
	Pending           string
	Running           string
	WaitingForDeploy  string
	WaitingForCutover string
	CuttingOver       string
	RevertWindow      string
	Completed         string
	Failed            string
	Stopped           string
	Cancelled         string
	Reverted          string

	// PlanetScale-specific states for the branch/deploy lifecycle.
	// These are set on the apply record during engine setup so the
	// progress handler and CLI can show what's happening.
	PreparingBranch       string
	ApplyingBranchChanges string
	CreatingDeployRequest string
}{
	Pending:           "pending",
	Running:           "running",
	WaitingForDeploy:  "waiting_for_deploy",
	WaitingForCutover: "waiting_for_cutover",
	CuttingOver:       "cutting_over",
	RevertWindow:      "revert_window",
	Completed:         "completed",
	Failed:            "failed",
	Stopped:           "stopped",
	Cancelled:         "cancelled",
	Reverted:          "reverted",

	PreparingBranch:       "preparing_branch",
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
//  7. All non-completed tasks WAITING_FOR_DEPLOY → Apply WAITING_FOR_DEPLOY
//  8. Any task REVERT_WINDOW → Apply REVERT_WINDOW
//  9. Any task RUNNING → Apply RUNNING
//  10. Otherwise → Apply PENDING
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
	waitingDeployOrCompleted := counts[Apply.WaitingForDeploy] + counts[Apply.Completed]
	if waitingDeployOrCompleted == total && counts[Apply.WaitingForDeploy] > 0 {
		return Apply.WaitingForDeploy
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
	case "WAITING_FOR_DEPLOY":
		return Apply.WaitingForDeploy
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
// where no further processing will occur.
func IsTerminalApplyState(s string) bool {
	switch s {
	case Apply.Completed, Apply.Failed, Apply.Stopped, Apply.Cancelled, Apply.Reverted:
		return true
	default:
		return false
	}
}

// IsBranchSetupPhase returns true if the apply state is a PlanetScale branch
// lifecycle phase where per-table progress is not yet meaningful (all tables
// are Queued). Used by the TUI and CLI to hide the table list during setup.
// WaitingForDeploy is included because the deploy hasn't started yet.
func IsBranchSetupPhase(s string) bool {
	return IsState(s, Apply.Pending, Apply.PreparingBranch, Apply.ApplyingBranchChanges, Apply.CreatingDeployRequest, Apply.WaitingForDeploy)
}

// IsPlanetScaleEngine returns true if the engine string indicates PlanetScale/Vitess.
// Handles display names ("PlanetScale"), storage constants ("planetscale"),
// and proto enum strings ("ENGINE_PLANETSCALE").
func IsPlanetScaleEngine(engine string) bool {
	return strings.EqualFold(engine, "planetscale") || strings.EqualFold(engine, "ENGINE_PLANETSCALE")
}
