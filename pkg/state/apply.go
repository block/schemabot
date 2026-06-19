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
	RunningDegraded   string
	Resuming          string
	WaitingForDeploy  string
	WaitingForCutover string
	Recovering        string
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
	PreparingBranch         string
	ApplyingBranchChanges   string
	ValidatingBranch        string
	CreatingDeployRequest   string
	ValidatingDeployRequest string
}{
	Pending:           "pending",
	Running:           "running",
	RunningDegraded:   "running_degraded",
	Resuming:          "resuming",
	WaitingForDeploy:  "waiting_for_deploy",
	WaitingForCutover: "waiting_for_cutover",
	Recovering:        "recovering",
	CuttingOver:       "cutting_over",
	RevertWindow:      "revert_window",
	Completed:         "completed",
	Failed:            "failed",
	FailedRetryable:   "failed_retryable",
	Stopped:           "stopped",
	Cancelled:         "cancelled",
	Reverted:          "reverted",

	PreparingBranch:         "preparing_branch",
	ApplyingBranchChanges:   "applying_branch_changes",
	ValidatingBranch:        "validating_branch",
	CreatingDeployRequest:   "creating_deploy_request",
	ValidatingDeployRequest: "validating_deploy_request",
}

// DeriveApplyState determines the overall Apply state from individual Task states.
//
// State priority (highest to lowest):
//  1. Any task FAILED → Apply FAILED
//  2. Any task FAILED_RETRYABLE → Apply FAILED_RETRYABLE
//  3. Any task STOPPED → Apply STOPPED
//  4. Any task REVERTED → Apply REVERTED
//  5. All tasks COMPLETED → Apply COMPLETED
//  6. Any task RECOVERING → Apply RECOVERING
//  7. Any task CUTTING_OVER → Apply CUTTING_OVER
//  8. All non-completed tasks WAITING_FOR_CUTOVER → Apply WAITING_FOR_CUTOVER
//  9. All non-completed tasks WAITING_FOR_DEPLOY → Apply WAITING_FOR_DEPLOY
//  10. Any task REVERT_WINDOW → Apply REVERT_WINDOW
//  11. Any task RUNNING → Apply RUNNING
//  12. Otherwise → Apply PENDING
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
	if counts[Apply.Recovering] > 0 {
		return Apply.Recovering
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

// RolloutChild is one apply_operation's contribution to the parent apply's
// rollout projection: its derived state plus whether the on_failure policy
// captured on that operation lets the rollout continue past a terminal failure.
//
// ContinueOnFailure must be set by the caller using the exact-match semantics of
// the claim predicate: only the literal on_failure value "continue" is
// continuable; "halt", "pause", and any unrecognized value are not, so the
// projection fails closed (a failed sibling keeps the apply failed) on anything
// but an explicit continue.
type RolloutChild struct {
	// State is the child operation's derived apply state.
	State string
	// ContinueOnFailure is true only when the operation's on_failure policy is
	// exactly "continue".
	ContinueOnFailure bool
}

// DeriveRolloutApplyState projects the parent apply's state over all of its
// child operations, accounting for the on_failure rollout-continuation policy.
//
// It builds on DeriveApplyState: the base projection is computed the same way,
// and any non-failed base is returned unchanged. The policy only modulates the
// failed case. continue governs rollout *continuation*, never the apply's
// pass/fail verdict — so an apply that suffered a continuable failure still
// settles to failed once every sibling is terminal; the policy only delays that
// verdict so the remaining siblings get their turn instead of the first failure
// terminalizing the whole apply.
//
// When the base projection is failed (at least one child terminally failed):
//
//   - if any failed child is not continuable (ContinueOnFailure false), the
//     failure stands and the apply is failed (fail closed, matching halt); else
//   - if every child is terminal, the rollout is settled and the apply is
//     failed (the verdict still reflects the failure); else
//   - the apply is held running_degraded so the still-in-flight siblings can
//     run to completion under continue while surfacing that a sibling has
//     already failed.
//
// An empty child set returns Pending, matching DeriveApplyState.
func DeriveRolloutApplyState(children []RolloutChild) string {
	if len(children) == 0 {
		return Apply.Pending
	}

	childStates := make([]string, len(children))
	for i, c := range children {
		childStates[i] = c.State
	}
	base := DeriveApplyState(childStates)
	if !IsState(base, Apply.Failed) {
		return base
	}

	allTerminal := true
	for _, c := range children {
		if IsState(c.State, Apply.Failed) && !c.ContinueOnFailure {
			return Apply.Failed
		}
		if !IsTerminalApplyState(c.State) {
			allTerminal = false
		}
	}
	if allTerminal {
		return Apply.Failed
	}
	return Apply.RunningDegraded
}

// normalizeApplyState converts a task state string to its canonical lowercase form.
func normalizeApplyState(raw string) string {
	switch strings.ToUpper(raw) {
	case "PENDING":
		return Apply.Pending
	case "RUNNING":
		return Apply.Running
	case "RUNNING_DEGRADED":
		return Apply.RunningDegraded
	case "WAITING_FOR_DEPLOY":
		return Apply.WaitingForDeploy
	case "WAITING_FOR_CUTOVER":
		return Apply.WaitingForCutover
	case "RECOVERING", "RECOVERING_CUTOVER":
		return Apply.Recovering
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
	case "VALIDATING_BRANCH":
		return Apply.ValidatingBranch
	case "VALIDATING_DEPLOY_REQUEST":
		return Apply.ValidatingDeployRequest
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
// where no further processing will occur. FailedRetryable is not terminal;
// operator drivers may claim and retry it.
// Accepts any format (proto "STATE_COMPLETED", uppercase "COMPLETED", or
// canonical lowercase "completed") — normalizes first.
func IsTerminalApplyState(s string) bool {
	info, ok := LookupApply(NormalizeState(s))
	return ok && info.Terminal
}

// IsRunningApplyState reports whether an apply is in a running-family state:
// running, or running_degraded (a continue rollout still in flight after a
// sibling deployment failed). Control gates that mean "the apply is actively
// running" — cutover readiness, start reconciliation, stop/volume eligibility —
// must use this so a degraded rollout is not mistaken for a non-running apply.
// This is narrower than "active" (non-terminal): pending, waiting_for_cutover,
// recovering, and other non-terminal states are not running-family.
// Accepts any format (proto, uppercase, or canonical lowercase).
func IsRunningApplyState(s string) bool {
	return IsState(s, Apply.Running, Apply.RunningDegraded)
}

// IsSetupPhase returns true if the apply state is an engine-lifecycle phase
// that runs before per-table progress is meaningful (all tables are Queued).
// Used by the TUI and CLI to hide the table list during setup.
// WaitingForDeploy is included because the deploy hasn't started yet.
func IsSetupPhase(s string) bool {
	info, ok := LookupApply(NormalizeState(s))
	return ok && info.SetupPhase
}

// IsPlanetScaleEngine returns true if the engine string indicates PlanetScale/Vitess.
// Handles display names ("PlanetScale"), storage constants ("planetscale"),
// and proto enum strings ("ENGINE_PLANETSCALE").
func IsPlanetScaleEngine(engine string) bool {
	return strings.EqualFold(engine, "planetscale") || strings.EqualFold(engine, "ENGINE_PLANETSCALE")
}
