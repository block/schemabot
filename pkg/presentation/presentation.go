// Package presentation derives the user-facing rollup for a multi-deployment
// apply from its apply_operations rows. It is surface-agnostic: the PR comment
// renderer and the Admin CLI both build their output from the same Apply model,
// so the aggregate header, per-deployment labels, ordering context, and default
// expand/collapse are computed once and rendered identically everywhere.
//
// The derivation adds no persisted state. Labels such as "halted",
// "ready for cutover — waiting for <deployment>", and "queued — next in order"
// exist only here; they are projections of (operation state, earlier-sibling
// states, resolved order, cutover_policy, halt_on_failure). The ordering
// context is derived from the same gate FindNextApplyOperation evaluates, so the
// presentation never contradicts what the operator will actually claim next.
//
// Vocabulary is deployment-facing only — the model never exposes the internal
// "apply_operation" term.
package presentation

import (
	"fmt"

	"github.com/block/schemabot/pkg/state"
)

// Operation is the minimal, surface-neutral input the derivation needs for one
// deployment of an apply. Callers map their own rows (storage.ApplyOperation for
// the operator/webhook path, an API response for the CLI path) into this shape,
// resolving the two rollout-policy booleans at the boundary so the derivation
// stays free of storage and wire types.
type Operation struct {
	// Deployment is the deployment name this operation targets.
	Deployment string

	// State is the canonical operation state (state.ApplyOperation, == state.Apply).
	State string

	// Barrier is true when the operation's cutover_policy is "barrier" (resolved
	// by the caller from storage.CutoverPolicyBarrier). Under barrier an earlier
	// sibling stops blocking a later copy once it reaches the cutover barrier or
	// succeeds; under rolling only a completed earlier sibling stops blocking.
	Barrier bool

	// HaltOnFailure is the resolved halt-on-failure policy (the caller derefs the
	// stored *bool, treating nil as true — the safe default). When false, a
	// terminal-failed earlier sibling no longer blocks later deployments.
	HaltOnFailure bool

	// ContinueOnFailure is true only when the operation's on_failure policy is
	// exactly "continue". The aggregate projection uses it to hold a rollout
	// running_degraded past a continuable sibling failure; halt, pause, empty,
	// and any unrecognized value must be false so the projection fails closed.
	ContinueOnFailure bool

	// Error is the operation's error detail, set when State is failed.
	Error string
}

// PresentationState is the semantic, render-time status of one deployment. It is
// richer than the raw operation state because the same state means different
// things depending on a deployment's earlier siblings (a pending operation may
// be next in order, waiting on an earlier copy, or halted by an earlier
// failure). Surfaces may switch on this for their own styling; Label/Emoji/Open
// carry the ready-to-render projection.
type PresentationState int

const (
	StateUnknown PresentationState = iota
	StateCompleted
	StateRunningCopy
	StateReadyForCutoverNext
	StateReadyForCutoverWaiting
	StateCuttingOver
	StateQueuedNext
	StateWaiting
	StateHalted
	StateFailed
	StateRetrying
	StateStopped
	StateRevertWindow
	StateCancelled
	StateReverted
)

// Deployment is the derived presentation for one deployment of the apply.
type Deployment struct {
	// Deployment is the deployment name.
	Deployment string

	// State is the raw operation state it was derived from.
	State string

	// Presentation is the semantic render-time status (see PresentationState).
	Presentation PresentationState

	// Label is the operator-facing summary, including any referenced deployment
	// (e.g. "ready for cutover — waiting for payments-eu", "halted — payments-us
	// failed"). It carries no progress percentage — fine-grained copy progress is
	// joined from the operation's tasks by the renderer (see the design note's
	// phase-source section).
	Label string

	// Emoji is the status glyph for the summary line.
	Emoji string

	// Open is the default expand/collapse for a details section: active or
	// problematic deployments default open, completed/queued default collapsed.
	Open bool

	// Error is the operation's error detail when it failed, for the renderer to
	// surface in the failed deployment's section.
	Error string
}

// NextActionKind is the semantic operator action the aggregate suggests. The
// derivation deliberately emits a semantic action plus its target deployment
// rather than a CLI command string, so each surface formats the actual verb and
// flags (and the verb set stays owned by the CLI, not duplicated here).
type NextActionKind int

const (
	// NextActionNone means no operator action is pending.
	NextActionNone NextActionKind = iota
	// NextActionCutover means the operator should cut over Deployment.
	NextActionCutover
	// NextActionResume means a stopped apply should be resumed.
	NextActionResume
	// NextActionReviewFailure means the operator should review the failed
	// Deployment before remediating.
	NextActionReviewFailure
)

// NextAction is the aggregate's suggested next operator action.
type NextAction struct {
	Kind NextActionKind
	// Deployment is the action's target, when the action is deployment-scoped.
	Deployment string
}

// StateCount is one entry of the aggregate's per-status histogram.
type StateCount struct {
	Label string
	Count int
}

// Apply is the surface-agnostic rollup for one logical apply.
type Apply struct {
	// State is the aggregate apply state derived from the child operation states
	// via state.DeriveRolloutApplyState — the same policy-aware projection that
	// backs applies.state, so a continue rollout still in flight past a failed
	// sibling shows running_degraded here too rather than a premature failed.
	State string

	// Label is the operator-facing aggregate status (e.g. "waiting for cutover").
	Label string

	// Counts is the per-status histogram in a stable display order, with zero
	// entries omitted, so the operator sees rollout health without expanding.
	Counts []StateCount

	// NextAction is the suggested next operator action, if any.
	NextAction NextAction

	// FirstFailure is the first deployment, in resolved order, whose operation
	// is terminally failed, or nil when no deployment has failed. It is the
	// rendering counterpart of the persisted aggregate ErrorMessage (which is
	// also stamped from the first failed operation): surfaces lift the failed
	// deployment and its error to the aggregate header so an operator sees what
	// failed without expanding a per-deployment section. Under on_failure
	// continue this is populated while the aggregate is still running, so the
	// failure surfaces eagerly rather than only on the terminal comment.
	FirstFailure *Deployment

	// Deployments are the per-deployment presentations in resolved deployment
	// order (the order the caller supplies, which mirrors the rollout order).
	Deployments []Deployment
}

// MultiDeployment reports whether the apply owns more than one deployment.
// Callers use it as the single↔multi render threshold: when false, the apply
// should render exactly as today's UX with no deployment hierarchy. It is
// derived from the deployment count rather than stored, so it can never drift
// from Deployments.
func (a Apply) MultiDeployment() bool {
	return len(a.Deployments) > 1
}

// Derive projects the ordered operations of one apply into its rollup. The input
// must be in resolved deployment order (as returned by ListByApply); earlier
// siblings are those before a given index.
func Derive(ops []Operation) Apply {
	children := make([]state.RolloutChild, len(ops))
	for i, op := range ops {
		children[i] = state.RolloutChild{
			State:             op.State,
			ContinueOnFailure: op.ContinueOnFailure,
		}
	}

	deployments := make([]Deployment, len(ops))
	for i := range ops {
		deployments[i] = deriveDeployment(ops, i)
	}

	aggState := state.DeriveRolloutApplyState(children)
	return Apply{
		State:        aggState,
		Label:        aggregateLabel(aggState),
		Counts:       summaryCounts(deployments),
		NextAction:   nextAction(aggState, deployments),
		FirstFailure: firstFailure(deployments),
		Deployments:  deployments,
	}
}

// firstFailure returns the first deployment, in resolved order, whose raw
// operation state is terminally failed, or nil when none failed. failed_retryable
// is excluded: a retrying deployment is still in progress and surfaces no
// operator-facing failure. The result is a copy, not an alias into Deployments,
// so a caller that later re-slices or sorts Deployments cannot turn it into a
// stale pointer.
func firstFailure(deps []Deployment) *Deployment {
	for i := range deps {
		if deps[i].State == state.ApplyOperation.Failed {
			failed := deps[i]
			return &failed
		}
	}
	return nil
}

// deriveDeployment projects operation i, using its earlier siblings for the
// ordering context that disambiguates pending and waiting_for_cutover.
func deriveDeployment(ops []Operation, i int) Deployment {
	op := ops[i]
	d := Deployment{Deployment: op.Deployment, State: op.State, Error: op.Error}

	switch op.State {
	case state.ApplyOperation.Completed:
		d.set(StateCompleted, "completed", "✅", false)
	case state.ApplyOperation.Running:
		d.set(StateRunningCopy, "running table copy", "🔄", true)
	case state.ApplyOperation.CuttingOver:
		d.set(StateCuttingOver, "cutting over", "🔁", true)
	case state.ApplyOperation.Failed:
		d.set(StateFailed, "failed", "❌", true)
	case state.ApplyOperation.FailedRetryable:
		d.set(StateRetrying, "retrying", "🔁", true)
	case state.ApplyOperation.Stopped:
		d.set(StateStopped, "stopped — resume to continue", "⏸", true)
	case state.ApplyOperation.RevertWindow:
		d.set(StateRevertWindow, "in revert window", "⏳", true)
	case state.ApplyOperation.Cancelled:
		d.set(StateCancelled, "cancelled", "⛔", false)
	case state.ApplyOperation.Reverted:
		d.set(StateReverted, "reverted", "↩️", false)
	case state.ApplyOperation.Pending:
		derivePending(&d, ops, i)
	case state.ApplyOperation.WaitingForCutover:
		deriveWaitingForCutover(&d, ops, i)
	default:
		// Unknown / engine-specific transient state: show it verbatim and keep it
		// open so an operator is never left without a status for a deployment.
		d.set(StateUnknown, op.State, "", true)
	}
	return d
}

// derivePending splits a pending operation into next-in-order, waiting-on, or
// halted, mirroring the pending sibling gate in FindNextApplyOperation so the
// label agrees with what the operator will claim next.
func derivePending(d *Deployment, ops []Operation, i int) {
	op := ops[i]
	if h := haltingBlocker(ops, i); h != nil {
		d.set(StateHalted, fmt.Sprintf("halted — %s %s", h.Deployment, haltedReason(h.State)), "⏸", true)
		return
	}
	for j := range i {
		if blocksPending(ops[j].State, op.Barrier, op.HaltOnFailure) {
			d.set(StateWaiting, fmt.Sprintf("waiting for %s", ops[j].Deployment), "⏳", false)
			return
		}
	}
	d.set(StateQueuedNext, "queued — next in order", "⏳", false)
}

// deriveWaitingForCutover splits a copied-and-parked operation into ready-now or
// ready-but-waiting-on-an-earlier-cutover. Cutover ordering is strict-complete
// and independent of cutover_policy, which only relaxes copy start.
func deriveWaitingForCutover(d *Deployment, ops []Operation, i int) {
	op := ops[i]
	for j := range i {
		if blocksCutover(ops[j].State, op.HaltOnFailure) {
			d.set(StateReadyForCutoverWaiting, fmt.Sprintf("ready for cutover — waiting for %s", ops[j].Deployment), "🟡", true)
			return
		}
	}
	d.set(StateReadyForCutoverNext, "ready for cutover — next in order", "🟢", true)
}

// haltingBlocker returns the earliest earlier sibling that halts the rollout for
// a pending operation: a terminal-failed sibling (only when halt_on_failure is
// on) or a cancelled/reverted sibling (which halt regardless, matching the
// predicate's lack of an exemption for them).
func haltingBlocker(ops []Operation, i int) *Operation {
	halt := ops[i].HaltOnFailure
	for j := range i {
		switch ops[j].State {
		case state.ApplyOperation.Failed:
			if halt {
				return &ops[j]
			}
		case state.ApplyOperation.Cancelled, state.ApplyOperation.Reverted:
			return &ops[j]
		}
	}
	return nil
}

// blocksPending reports whether an earlier sibling in earlierState blocks a
// pending operation from being claimed. It mirrors the SQL sibling gate in
// FindNextApplyOperation exactly: the cutover_policy branch selects the
// non-blocking set, then the halt_on_failure exemption is applied.
func blocksPending(earlierState string, barrier, halt bool) bool {
	var policyBlocks bool
	if barrier {
		policyBlocks = !isBarrierNonBlocking(earlierState)
	} else {
		policyBlocks = earlierState != state.ApplyOperation.Completed
	}
	if !policyBlocks {
		return false
	}
	// halt_on_failure exemption: a terminal-failed earlier sibling stops blocking
	// when the policy is off.
	if !halt && earlierState == state.ApplyOperation.Failed {
		return false
	}
	return true
}

// isBarrierNonBlocking reports whether an earlier sibling state is in the set
// that, under cutover_policy=barrier, no longer blocks a later copy: it has
// reached the cutover barrier or succeeded. This must match the IN-list in
// FindNextApplyOperation's barrier branch (waiting_for_cutover, cutting_over,
// revert_window, completed).
func isBarrierNonBlocking(s string) bool {
	switch s {
	case state.ApplyOperation.WaitingForCutover,
		state.ApplyOperation.CuttingOver,
		state.ApplyOperation.RevertWindow,
		state.ApplyOperation.Completed:
		return true
	default:
		return false
	}
}

// blocksCutover reports whether an earlier sibling in earlierState blocks a
// later deployment's ordered cutover. Cutover is strictly ordered: an earlier
// sibling blocks until it is completed, except a terminal-failed sibling under
// halt_on_failure=false, which the rollout is configured to continue past.
func blocksCutover(earlierState string, halt bool) bool {
	if earlierState == state.ApplyOperation.Completed {
		return false
	}
	if earlierState == state.ApplyOperation.Failed && !halt {
		return false
	}
	return true
}

// haltedReason returns the deployment-facing reason word for a halting blocker.
func haltedReason(blockerState string) string {
	switch blockerState {
	case state.ApplyOperation.Failed:
		return "failed"
	case state.ApplyOperation.Cancelled:
		return "cancelled"
	case state.ApplyOperation.Reverted:
		return "reverted"
	default:
		return blockerState
	}
}

// aggregateLabel glosses the derived aggregate state into an operator-facing
// status word.
func aggregateLabel(s string) string {
	switch s {
	case state.Apply.Pending:
		return "queued"
	case state.Apply.Running:
		return "running"
	case state.Apply.RunningDegraded:
		return "running (degraded)"
	case state.Apply.WaitingForDeploy:
		return "waiting for deploy"
	case state.Apply.WaitingForCutover:
		return "waiting for cutover"
	case state.Apply.CuttingOver:
		return "cutting over"
	case state.Apply.RevertWindow:
		return "in revert window"
	case state.Apply.Completed:
		return "completed"
	case state.Apply.Failed:
		return "failed"
	case state.Apply.FailedRetryable:
		return "retrying"
	case state.Apply.Stopped:
		return "stopped"
	case state.Apply.Cancelled:
		return "cancelled"
	case state.Apply.Reverted:
		return "reverted"
	default:
		return s
	}
}

// nextAction derives the aggregate's suggested operator action from the
// aggregate state and the per-deployment presentations, in operator-priority
// order: a failure to review, then a stop to resume, then an available cutover.
func nextAction(aggState string, deps []Deployment) NextAction {
	// A failed rollout is fail-closed: review the failure before anything else,
	// even if an earlier deployment is sitting ready for cutover.
	if aggState == state.Apply.Failed {
		if d, ok := firstWithState(deps, state.ApplyOperation.Failed); ok {
			return NextAction{Kind: NextActionReviewFailure, Deployment: d.Deployment}
		}
		return NextAction{Kind: NextActionReviewFailure}
	}
	// A deliberately stopped rollout resumes before any cutover is offered.
	if aggState == state.Apply.Stopped {
		return NextAction{Kind: NextActionResume}
	}
	// A deployment that is next in order for cutover is actionable even while the
	// aggregate is still running — an earlier deployment can cut over while a
	// later one is still copying (waiting_for_cutover does not roll the aggregate
	// up until every deployment has reached it). This covers both the
	// waiting_for_cutover aggregate and the running-with-one-ready case.
	if d, ok := firstWithPresentation(deps, StateReadyForCutoverNext); ok {
		return NextAction{Kind: NextActionCutover, Deployment: d.Deployment}
	}
	return NextAction{Kind: NextActionNone}
}

// summaryCategoryOrder is the stable display order for the aggregate histogram.
var summaryCategoryOrder = []struct {
	label  string
	states []PresentationState
}{
	{"completed", []PresentationState{StateCompleted}},
	{"cutting over", []PresentationState{StateCuttingOver}},
	{"ready for cutover", []PresentationState{StateReadyForCutoverNext, StateReadyForCutoverWaiting}},
	{"running", []PresentationState{StateRunningCopy}},
	{"waiting", []PresentationState{StateWaiting}},
	{"queued", []PresentationState{StateQueuedNext}},
	{"halted", []PresentationState{StateHalted}},
	{"failed", []PresentationState{StateFailed}},
	{"retrying", []PresentationState{StateRetrying}},
	{"stopped", []PresentationState{StateStopped}},
	{"in revert window", []PresentationState{StateRevertWindow}},
	{"cancelled", []PresentationState{StateCancelled}},
	{"reverted", []PresentationState{StateReverted}},
	{"unknown", []PresentationState{StateUnknown}},
}

// summaryCounts builds the per-status histogram in summaryCategoryOrder, omitting
// zero entries.
func summaryCounts(deps []Deployment) []StateCount {
	tally := make(map[PresentationState]int, len(deps))
	for _, d := range deps {
		tally[d.Presentation]++
	}
	var counts []StateCount
	for _, cat := range summaryCategoryOrder {
		n := 0
		for _, ps := range cat.states {
			n += tally[ps]
		}
		if n > 0 {
			counts = append(counts, StateCount{Label: cat.label, Count: n})
		}
	}
	return counts
}

func firstWithState(deps []Deployment, s string) (Deployment, bool) {
	for _, d := range deps {
		if d.State == s {
			return d, true
		}
	}
	return Deployment{}, false
}

func firstWithPresentation(deps []Deployment, ps PresentationState) (Deployment, bool) {
	for _, d := range deps {
		if d.Presentation == ps {
			return d, true
		}
	}
	return Deployment{}, false
}

func (d *Deployment) set(ps PresentationState, label, emoji string, open bool) {
	d.Presentation = ps
	d.Label = label
	d.Emoji = emoji
	d.Open = open
}
