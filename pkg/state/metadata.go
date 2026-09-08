package state

// ApplyStateInfo holds presentation-neutral metadata for a canonical Apply state.
//
// Renderers (CLI, TUI, PR comments) reach for Label when they need a short
// human-readable name. Control-plane code reaches for Terminal and SetupPhase
// to make scheduling and gating decisions. Centralizing this metadata avoids
// the drift that occurs when each consumer maintains its own switch.
//
// Engine-specific or surface-specific copy (Title Case headers, color, emoji)
// is intentionally NOT stored here — those remain local to the rendering layer
// so this registry does not overfit to one presentation.
type ApplyStateInfo struct {
	// Label is the canonical human-readable display name (sentence case),
	// e.g. "Waiting for deploy", "Cutting over", "Retrying".
	Label string

	// Terminal is true for states where no further processing will occur.
	// FailedRetryable is NOT terminal — the operator may retry it.
	// Stopped IS terminal at the apply level (operators must explicitly resume).
	Terminal bool

	// SetupPhase is true for engine-lifecycle phases that run before per-table
	// work has meaningfully started (all tables are still queued). Used by
	// the CLI and TUI to suppress the table list during setup. Engines that
	// stage work before the per-table phase (e.g. PlanetScale's branch and
	// deploy-request preparation) flag those states here; Pending and
	// WaitingForDeploy are included because the deploy hasn't started yet.
	SetupPhase bool

	// Hold says what an apply in this state does to the database it is holding,
	// for the surfaces that must refuse a second change on that database. It
	// continues a sentence that has already named the holding change, e.g.
	// "... is held by a stopped schema change; it holds the database until it
	// is started or cancelled".
	//
	// The distinction it draws is the one that tells an operator whether to act
	// or to wait: a change that holds the database until someone decides its
	// fate, versus one that releases it on its own. The resting states differ
	// only in which decision is owed. The revert window is the one that offers
	// no cancel — the change has already cut over, so stop and cancel are
	// permanently rejected there — and also the one whose wait is bounded
	// without an operator, since the window's expiry triggers the skip-revert
	// itself rather than being a third way out. A running apply promises
	// progress rather than release, because a deferred or ordered cutover parks
	// it at the barrier instead of finishing it.
	//
	// Empty for states whose next move is not certain (pending, cutting over,
	// recovering, waiting for a deploy the engine may be about to trigger
	// itself). Naming such a state is honest; inventing an action for it would
	// send an operator the wrong way, so callers render the state alone.
	Hold string
}

// The Hold promises shared by more than one state, kept as one string each so
// every state that releases the database on the same terms words it the same
// way.
const (
	holdUntilRevertFinishes = "it releases the database when the revert finishes, which it does on its own"
	holdUntilApplyFinishes  = "it releases the database when it finishes, unless it parks for cutover first"
)

// applyMetadata is the registry of metadata for every canonical Apply state.
// Every value of Apply.* must appear here. The metadata_test invariant
// enforces this so a newly added state cannot silently miss a label or
// classification.
var applyMetadata = map[string]ApplyStateInfo{
	Apply.Pending: {
		Label:      "Pending",
		SetupPhase: true,
	},
	Apply.Running: {
		Label: "Running",
		Hold:  holdUntilApplyFinishes,
	},
	Apply.RunningDegraded: {
		Label: "Running (degraded)",
		Hold:  holdUntilApplyFinishes,
	},
	Apply.CatchingUp: {
		Label: "Catching up",
		Hold:  holdUntilApplyFinishes,
	},
	Apply.Checksumming: {
		Label: "Checksumming",
		Hold:  holdUntilApplyFinishes,
	},
	Apply.PostChecksum: {
		Label: "Applying final changes",
		Hold:  holdUntilApplyFinishes,
	},
	Apply.Paused: {
		Label: "Paused",
	},
	Apply.Resuming: {
		Label: "Resuming",
	},
	Apply.WaitingForDeploy: {
		Label:      "Waiting for deploy",
		SetupPhase: true,
	},
	Apply.WaitingForCutover: {
		Label: "Waiting for cutover",
		Hold:  "it holds the database until it is cut over or cancelled",
	},
	Apply.Recovering: {
		Label: "Recovering",
	},
	Apply.CuttingOver: {
		Label: "Cutting over",
	},
	Apply.RevertWindow: {
		Label: "Revert window",
		Hold:  "it holds the database until it is reverted or skip-reverted",
	},
	Apply.SkippingRevert: {
		Label: "Skipping revert",
		Hold:  holdUntilRevertFinishes,
	},
	Apply.Reverting: {
		Label: "Reverting",
		Hold:  holdUntilRevertFinishes,
	},
	Apply.Completed: {
		Label:    "Completed",
		Terminal: true,
	},
	Apply.Failed: {
		Label:    "Failed",
		Terminal: true,
	},
	Apply.FailedRetryable: {
		Label: "Retrying",
		Hold:  "it holds the database until it is retried or cancelled",
	},
	Apply.Stopped: {
		Label:    "Stopped",
		Terminal: true,
		Hold:     "it holds the database until it is started or cancelled",
	},
	Apply.Cancelled: {
		Label:    "Cancelled",
		Terminal: true,
	},
	Apply.Reverted: {
		Label:    "Reverted",
		Terminal: true,
	},
	Apply.PreparingBranch: {
		Label:      "Preparing branch",
		SetupPhase: true,
	},
	Apply.ApplyingBranchChanges: {
		Label:      "Applying changes to branch",
		SetupPhase: true,
	},
	Apply.ValidatingBranch: {
		Label:      "Validating branch",
		SetupPhase: true,
	},
	Apply.CreatingDeployRequest: {
		Label:      "Creating deploy request",
		SetupPhase: true,
	},
	Apply.ValidatingDeployRequest: {
		Label:      "Validating deploy request",
		SetupPhase: true,
	},
}

// LookupApply returns the metadata for the given Apply state and whether the
// state is known to the registry. Unknown states return the zero ApplyStateInfo
// and false; callers decide whether to fall back to the raw state string or
// treat the unknown state as an error.
func LookupApply(s string) (ApplyStateInfo, bool) {
	info, ok := applyMetadata[s]
	return info, ok
}

// Label returns the canonical human-readable label for an Apply state, or the
// state string itself when the state is not in the registry. Used by CLI, TUI,
// and PR templates where a short label is needed; surface-specific titles
// (e.g. Title Case PR headers) remain local to the rendering layer.
func Label(s string) string {
	if info, ok := applyMetadata[s]; ok {
		return info.Label
	}
	return s
}

// Hold returns what an apply in this state does to the database it holds, for
// the surfaces that must explain a refused change (see ApplyStateInfo.Hold).
// Empty for a state whose next move is not certain, and for one the registry
// does not know — a caller that gets "" renders the state on its own rather
// than promising the operator an outcome.
// Accepts any format (proto, uppercase, or canonical lowercase).
func Hold(s string) string {
	info, ok := LookupApply(NormalizeState(s))
	if !ok {
		return ""
	}
	return info.Hold
}
