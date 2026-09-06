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
	Pending         string
	Running         string
	RunningDegraded string

	// Post-copy phases (Spirit): the row copy is done and the engine is
	// draining its accumulated changeset or verifying the copied data.
	// These mirror the Task states of the same name so the apply-level
	// state names the phase the whole apply is in once no table is still
	// copying rows. All three are running-family (IsRunningApplyState).
	CatchingUp   string
	Checksumming string
	PostChecksum string

	Paused            string
	Resuming          string
	WaitingForDeploy  string
	WaitingForCutover string
	Recovering        string
	CuttingOver       string
	RevertWindow      string
	SkippingRevert    string
	Reverting         string
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
	Pending:         "pending",
	Running:         "running",
	RunningDegraded: "running_degraded",

	CatchingUp:   "catching_up",
	Checksumming: "checksumming",
	PostChecksum: "post_checksum",

	Paused:            "paused",
	Resuming:          "resuming",
	WaitingForDeploy:  "waiting_for_deploy",
	WaitingForCutover: "waiting_for_cutover",
	Recovering:        "recovering",
	CuttingOver:       "cutting_over",
	RevertWindow:      "revert_window",
	SkippingRevert:    "skipping_revert",
	Reverting:         "reverting",
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
//  7. Any task CUTTING_OVER, no task in an earlier active phase (PENDING,
//     RUNNING, or a post-copy verification phase) → Apply CUTTING_OVER
//  8. All non-completed tasks WAITING_FOR_CUTOVER → Apply WAITING_FOR_CUTOVER
//  9. All non-completed tasks WAITING_FOR_DEPLOY → Apply WAITING_FOR_DEPLOY
//  10. Any task REVERT_WINDOW → Apply REVERT_WINDOW
//  11. Any task RUNNING → Apply RUNNING
//  12. Any task in a post-copy phase or cutting over while any task is still
//     PENDING → Apply RUNNING
//  13. Any task CATCHING_UP → Apply CATCHING_UP
//  14. Any task CHECKSUMMING → Apply CHECKSUMMING
//  15. Any task POST_CHECKSUM → Apply POST_CHECKSUM
//  16. Otherwise → Apply PENDING
//
// The post-copy phases (13–15) surface the least-advanced active phase, and
// only once every table has started: while any table is still copying rows —
// or still queued with its whole copy ahead of it — the apply is Running.
// Once every table has at least begun and the active ones are draining or
// verifying, the apply names that phase. The cutover gate (7) resolves
// least-advanced-first the same way: a drive cuts tables over as each
// finishes — sequentially, rolling, or across concurrent shards — so a
// table cutting over ahead of siblings that are still queued, copying, or
// verifying keeps the apply on that earlier work rather than announcing a
// cutover most tables have not reached. This keeps the derived state
// monotone across a multi-table drive — it never has to fall back from
// cutting_over to an earlier phase when a cutover completes ahead of its
// siblings.
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
	if counts[Apply.Reverting] > 0 {
		return Apply.Reverting
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
	if cutoverIsLeastAdvancedActiveWork(counts) {
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
	if postCopyPhaseWithQueuedWork(counts) {
		return Apply.Running
	}
	if counts[Apply.CatchingUp] > 0 {
		return Apply.CatchingUp
	}
	if counts[Apply.Checksumming] > 0 {
		return Apply.Checksumming
	}
	if counts[Apply.PostChecksum] > 0 {
		return Apply.PostChecksum
	}
	return Apply.Pending
}

// cutoverIsLeastAdvancedActiveWork reports whether a task is cutting over
// with no sibling in an earlier active phase — queued, copying, or verifying.
// A cutover is the last step of a table's work, so surfacing it at the apply
// level while earlier work is still active would overstate progress and force
// the derived state to fall back once that cutover completes; it surfaces
// only when it is the least advanced work left. A parked WAITING_FOR_CUTOVER
// sibling does not hold a cutover back: it is waiting on a command, not
// working.
func cutoverIsLeastAdvancedActiveWork(counts map[string]int) bool {
	return counts[Apply.CuttingOver] > 0 &&
		counts[Apply.Pending] == 0 &&
		counts[Apply.Running] == 0 &&
		counts[Apply.CatchingUp] == 0 &&
		counts[Apply.Checksumming] == 0 &&
		counts[Apply.PostChecksum] == 0
}

// postCopyPhaseWithQueuedWork reports whether a task is draining, verifying
// (catching up, checksumming, or post-checksum), or cutting over while
// another task has not started. Naming the phase at the apply level would
// overstate progress — the queued tables still have their whole copy ahead —
// so the apply stays Running until every table has begun.
func postCopyPhaseWithQueuedWork(counts map[string]int) bool {
	if counts[Apply.Pending] == 0 {
		return false
	}
	return counts[Apply.CatchingUp] > 0 || counts[Apply.Checksumming] > 0 ||
		counts[Apply.PostChecksum] > 0 || counts[Apply.CuttingOver] > 0
}

// RolloutChild is one apply_operation's contribution to the parent apply's
// rollout projection: its derived state plus how the on_failure policy captured
// on that operation treats a terminal failure of this child.
//
// The caller sets the two flags using the exact-match semantics of the claim
// predicate, so the projection mirrors what the operator will actually do:
//
//   - ContinueOnFailure: the failure does not block later siblings and does not
//     force an immediate terminal verdict. True for on_failure "continue", and
//     for on_failure "pause" once the rollout has been released (a released
//     pause behaves like continue for ordering).
//   - PauseOnFailure: the failure holds the rollout for a human. True for
//     on_failure "pause" before release.
//
// Both default to false, so "halt" and any unrecognized policy fail closed: a
// failed sibling keeps the apply failed. The two flags are mutually exclusive —
// a child is either continuable, pause-held, or fail-closed. Setting both is a
// caller bug; the projection fails closed in that case rather than loosening
// rollout gating.
//
// Children must be supplied in deployment order (the same (created_at, id) order
// the claim predicate uses), because a pause-held failure only holds the rollout
// when there is later, not-yet-terminal work to hold.
type RolloutChild struct {
	// State is the child operation's derived apply state.
	State string
	// ContinueOnFailure is true when the operation's on_failure policy lets the
	// rollout continue past this child's terminal failure ("continue", or a
	// released "pause").
	ContinueOnFailure bool
	// PauseOnFailure is true when the operation's on_failure policy is an
	// unreleased "pause": a terminal failure holds the rollout for a human.
	PauseOnFailure bool
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
// When the base projection is failed (at least one child terminally failed),
// each failed child is classified by its policy flags:
//
//   - halt or unrecognized (both flags false): the failure stands and the apply
//     is failed (fail closed); this dominates every other outcome.
//   - unreleased pause (PauseOnFailure) with later work that still holds its
//     target: the apply is held paused so a human can release or stop it.
//   - continue, or a released pause (ContinueOnFailure): the failure neither
//     forces a terminal verdict nor holds the rollout.
//
// After classifying every child, in precedence order:
//
//   - any fail-closed child → failed, or running_degraded while a sibling that
//     already started still holds its target (fail closed decides the verdict,
//     not when it is recorded);
//   - else any pause-held child → paused;
//   - else if every child has settled → failed (the verdict still reflects the
//     failure once the continue/released rollout has settled);
//   - else → running_degraded (siblings still holding their targets).
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

	allSettled := true
	hardFail := false
	pausedHold := false
	for i, c := range children {
		if childHoldsItsTarget(c) {
			allSettled = false
		}
		if !IsState(c.State, Apply.Failed) {
			continue
		}
		switch {
		case c.ContinueOnFailure && c.PauseOnFailure:
			// Invalid: the flags are mutually exclusive. Fail closed rather than
			// let a caller bug loosen rollout gating by silently winning the
			// continue branch below.
			hardFail = true
		case c.ContinueOnFailure:
			// continue, or a released pause: does not force a terminal verdict
			// and does not hold the rollout.
		case c.PauseOnFailure && hasLaterUnsettled(children, i):
			// unreleased pause with later work still to run: hold for a human.
			pausedHold = true
		case c.PauseOnFailure:
			// unreleased pause with nothing later to hold: the rollout is
			// effectively settled, so let the terminal/degraded checks below
			// decide rather than forcing failed here.
		default:
			// halt or any unrecognized policy: fail closed.
			hardFail = true
		}
	}
	if hardFail {
		// The verdict is decided, but the apply is not over. A fail-closed
		// policy only refuses new claims; it cancels nothing, so a sibling
		// that a driver already started keeps writing to its target. Recording
		// the terminal verdict over it would release the reservation on the
		// parent's whole target set (OW-5) while one of those targets is
		// mid-change, and would take stop and cancel away from the operator who
		// still has live work to stop. Hold the apply until that work settles.
		// A sibling still pending holds nothing: the same policy is what stops
		// it from ever starting. A sibling an operator stopped still holds its
		// target, because it can be started again.
		if hasStartedUnsettledWork(children) {
			return Apply.RunningDegraded
		}
		return Apply.Failed
	}
	if pausedHold {
		return Apply.Paused
	}
	if allSettled {
		return Apply.Failed
	}
	return Apply.RunningDegraded
}

// childHoldsItsTarget reports whether a child still holds the deployment it was
// given: its verdict is not final, so a driver may still be writing to that
// target or may claim it and start writing again.
//
// Settled rather than terminal is what draws this line, and the difference is
// one state. Stopped is terminal for claiming but not settled, because an
// operator can start it again and a driver will resume writing — which is what
// its hold says, that it holds the database until it is started or cancelled.
// A rollout that reads terminal here would release the reservation on its whole
// target set (OW-5) over a deployment a stopped sibling still owns.
func childHoldsItsTarget(c RolloutChild) bool {
	return !IsState(c.State, SettledApplyStates...)
}

// hasStartedUnsettledWork reports whether any child has moved past pending and
// still holds its target: work a driver has already begun.
//
// This is the line a fail-closed rollout turns on, because the two kinds of
// sibling differ in whether the policy reaches them. A pending sibling is
// exactly what the ordered-claim gate holds back, so it will not start while
// the failure stands and it has touched nothing. A sibling already running,
// draining, parked at a cutover barrier, awaiting a retry, or stopped by an
// operator was claimed before the failure, and refusing new claims does not
// reach back to release it.
func hasStartedUnsettledWork(children []RolloutChild) bool {
	for _, c := range children {
		if childHoldsItsTarget(c) && !IsState(c.State, Apply.Pending) {
			return true
		}
	}
	return false
}

// RolloutHeldByResumableChild reports whether a non-terminal projection is held
// open only by children an operator can start again.
//
// Every child has reached a terminal state, so no drive will move the parent on
// its own, and at least one has not settled: a stopped child still holds its
// target and resumes writing on start. That shape is indistinguishable from a
// stranded parent — one whose children all settled while the projection that
// should have recorded their outcome never ran — from the child rows alone, and
// the two want opposite handling. A stranded parent needs its state re-derived
// and its target released. This one is already showing the state it should, and
// waits on the operator who stopped it.
func RolloutHeldByResumableChild(derived string, children []RolloutChild) bool {
	if IsTerminalApplyState(derived) {
		return false
	}
	held := false
	for _, c := range children {
		if !IsTerminalApplyState(c.State) {
			return false
		}
		if childHoldsItsTarget(c) {
			held = true
		}
	}
	return held
}

// hasLaterUnsettled reports whether any child after failedIndex (in deployment
// order) still holds its target. A pause-held failure only holds the rollout
// when there is such later work for the operator to release, start or stop.
func hasLaterUnsettled(children []RolloutChild, failedIndex int) bool {
	for later := failedIndex + 1; later < len(children); later++ {
		if childHoldsItsTarget(children[later]) {
			return true
		}
	}
	return false
}

// normalizeApplyState converts a task state string to its canonical lowercase form.
func normalizeApplyState(raw string) string {
	switch strings.ToUpper(raw) {
	case "PENDING":
		return Apply.Pending
	case "RUNNING":
		return Apply.Running
	case "CATCHING_UP":
		return Apply.CatchingUp
	case "CHECKSUMMING":
		return Apply.Checksumming
	case "POST_CHECKSUM":
		return Apply.PostChecksum
	case "RUNNING_DEGRADED":
		return Apply.RunningDegraded
	case "PAUSED":
		return Apply.Paused
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
	case "REVERTING":
		return Apply.Reverting
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

// SettledApplyStates lists the apply states whose outcome can no longer change.
// It is the terminal set minus Stopped: a stopped apply is terminal but still
// addressable, so a driver may claim it and resume writing its child rows.
//
// Terminal is not settled, and the difference decides who may write. Anything
// that writes rows belonging to an apply it does not hold a lease on — a reaper
// closing out stranded children — must gate on settled, so it can never touch
// rows a driver is about to own.
//
// Settled bounds who may write; it does not promise the apply's child rows have
// stopped moving. One failed task settles its apply to failed while its siblings
// keep copying, so a reaper gating on this set still needs its own quiescence
// window before it may touch a child row.
var SettledApplyStates = []string{
	Apply.Completed,
	Apply.Failed,
	Apply.Cancelled,
	Apply.Reverted,
}

// IsRunningApplyState reports whether an apply is in a running-family state:
// running, running_degraded (a rollout still in flight past a failed sibling,
// whether it is continuing or halted), or one of the post-copy phases (catching_up,
// checksumming, post_checksum) where the engine is still actively working the
// change. Control gates that mean "the apply is actively running" — cutover
// readiness, start reconciliation, stop eligibility — must use this so
// a degraded rollout or a table draining its changeset is not mistaken for a
// non-running apply.
// This is narrower than "active" (non-terminal): pending, waiting_for_cutover,
// recovering, and other non-terminal states are not running-family.
// Accepts any format (proto, uppercase, or canonical lowercase).
func IsRunningApplyState(s string) bool {
	return IsState(s, Apply.Running, Apply.RunningDegraded,
		Apply.CatchingUp, Apply.Checksumming, Apply.PostChecksum)
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

// InitialActiveApplyState returns the state an apply enters the moment its work
// is dispatched to the engine. Spirit begins copying rows immediately, so its
// first active phase is Running. PlanetScale begins by preparing a branch and
// staging a deploy request, so its first active phase is PreparingBranch.
//
// Dispatch must use this instead of hardcoding Running: stamping Running on a
// PlanetScale apply that is only preparing its branch misrepresents the engine
// lifecycle and outranks the real deploy-request phases the engine later
// reports, pinning the stored state — and every surface that renders it — at a
// row-copy phase that has not begun.
func InitialActiveApplyState(engine string) string {
	if IsPlanetScaleEngine(engine) {
		return Apply.PreparingBranch
	}
	return Apply.Running
}
