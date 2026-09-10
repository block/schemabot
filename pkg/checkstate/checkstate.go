// Package checkstate holds the vocabulary of SchemaBot's stored check state and
// the reading of it an operator needs during triage.
//
// A GitHub Check Run is the visible object on a PR commit. Stored check state is
// the row SchemaBot keeps to decide what that Check Run should say. The two can
// disagree, and when they do the question is always the same: is this a state
// SchemaBot will resolve on its own, or one waiting on a person? Answering it
// from the row is what this package is for, so the answer does not have to be
// re-derived from server logs.
//
// Every reading here errs toward "waiting on a person". Telling an operator to
// wait on something that will never resolve costs far more than telling them to
// look at something that would have cleared anyway.
package checkstate

import (
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// GitHub Check Run status values.
const (
	StatusQueued     = "queued"
	StatusInProgress = "in_progress"
	StatusCompleted  = "completed"
)

// GitHub Check Run conclusion values.
const (
	ConclusionSuccess        = "success"
	ConclusionFailure        = "failure"
	ConclusionActionRequired = "action_required"
	ConclusionNeutral        = "neutral"
	ConclusionSkipped        = "skipped"
)

// ConclusionClearsGate reports whether a completed Check Run's conclusion lets
// branch protection pass.
//
// Only the conclusions GitHub itself treats as passing clear it. Anything else
// holds the gate, including a value this list does not know: a conclusion that
// cannot be read as passing is not one to report as passing.
func ConclusionClearsGate(conclusion string) bool {
	switch conclusion {
	case ConclusionSuccess, ConclusionNeutral, ConclusionSkipped:
		return true
	default:
		return false
	}
}

// AggregateSentinel is the value the aggregate row carries in place of both a
// database type and a database name. The aggregate is stored check state like
// any other row, but it rolls up the per-database rows rather than describing a
// database of its own.
const AggregateSentinel = "_aggregate"

// IsAggregate reports whether a stored row is the rollup rather than a
// database's own result. Both sentinel fields have to match: a database
// legitimately named like the sentinel is still a database, and reading its
// result as a rollup would attribute a real finding to a row that carries none.
func IsAggregate(check *storage.Check) bool {
	return check.DatabaseType == AggregateSentinel && check.DatabaseName == AggregateSentinel
}

// Durable blocking reasons recorded on a check row. A writer sets one when the
// block has to survive later writes that did not re-evaluate the condition, so
// a non-empty value is always a deliberate decision to keep the gate closed.
const (
	BlockSchemaRemovedAfterApplyStarted   = "schema_removed_after_apply_started"
	BlockRollbackCompleted                = "rollback_completed"
	BlockApplyCancelled                   = "apply_cancelled"
	BlockApplyCancelledAfterTaskCompleted = "apply_cancelled_after_task_completed"
	BlockConfigDiscoveryUnavailable       = "github_schema_config_discovery_unavailable"
	BlockConfigDiscoveryFailed            = "schema_config_discovery_failed"
	BlockPlanPublishVerificationFailed    = "plan_publish_verification_failed"
	BlockPRFileCapExceeded                = "pr_file_cap_exceeded"
	BlockManagedDirMissingConfig          = "managed_dir_missing_config"
	BlockNoAllowedConfiguredEnvironments  = "no_allowed_configured_environments"
	BlockParticipantUnresolved            = "participant_unresolved"
	BlockReviewTimeDeploymentDrift        = storage.ReviewTimeDeploymentDriftBlockingReason
)

// blockClass is what a durable blocking reason means for the operator reading
// it: who has to move, and whether waiting is ever the answer.
type blockClass int

const (
	// blockGuard: a condition outside the check failed it closed. Something a
	// person controls has to change before a re-plan can lift it.
	blockGuard blockClass = iota
	// blockReconciliation: work may have reached the target, so the target and
	// the pull request may no longer describe the same schema. No plan and no
	// new commit settles that question.
	blockReconciliation
	// blockAwaitingReport: another deployment owes a result. SchemaBot lifts
	// the block when that result arrives.
	blockAwaitingReport
)

// blockClasses classifies every durable blocking reason SchemaBot writes.
//
// Review-time deployment drift is a guard rather than a reconciliation: the
// deployments may simply have moved on from the reviewed plan, and a re-plan
// that re-evaluates the rollup lifts the block once they match. Reading it as
// a reconciliation would deny the action that actually clears it.
//
// An unlisted reason is read as blockGuard: the column is a plain string, so a
// value this table does not know about is one whose remedy cannot be asserted,
// and the honest reading of an unknown block is that someone has to look at it.
// The completeness test over the writers' own registry keeps a newly added
// reason from silently taking that default.
var blockClasses = map[string]blockClass{
	BlockSchemaRemovedAfterApplyStarted:   blockReconciliation,
	BlockRollbackCompleted:                blockReconciliation,
	BlockApplyCancelledAfterTaskCompleted: blockReconciliation,
	BlockApplyCancelled:                   blockGuard,
	BlockReviewTimeDeploymentDrift:        blockGuard,
	BlockConfigDiscoveryUnavailable:       blockGuard,
	BlockConfigDiscoveryFailed:            blockGuard,
	BlockPlanPublishVerificationFailed:    blockGuard,
	BlockPRFileCapExceeded:                blockGuard,
	BlockManagedDirMissingConfig:          blockGuard,
	BlockNoAllowedConfiguredEnvironments:  blockGuard,
	BlockParticipantUnresolved:            blockAwaitingReport,
}

// BlockingReasonIsClassified reports whether a durable blocking reason has an
// asserted remedy rather than falling back to the unknown-block default.
func BlockingReasonIsClassified(reason string) bool {
	_, ok := blockClasses[reason]
	return ok
}

// Reason codes for why a stored check row reads the way it does on a given
// commit. They are stable identifiers: an operator greps for one and a script
// branches on it.
const (
	// ReasonResolved: the row covers the commit and concluded successfully.
	ReasonResolved = "resolved"
	// ReasonBlocked: the row covers the commit and concluded blocking. The
	// Check Run already says why.
	ReasonBlocked = "blocked"
	// ReasonGuardBlocked: a durable guard failed this row closed. It stays
	// blocking until the condition behind it changes and a plan re-runs it.
	ReasonGuardBlocked = "guard_blocked"
	// ReasonApplyRunning: an apply owns the row and is running against the
	// commit the PR is gated on.
	ReasonApplyRunning = "apply_running"
	// ReasonApplyRunningOnOlderCommit: an apply owns the row and is running
	// against an earlier commit. The gate waits for it to settle.
	ReasonApplyRunningOnOlderCommit = "apply_running_on_older_commit"
	// ReasonApplyStopped: an apply owns the row and is stopped. It will not
	// settle until an operator starts or cancels it.
	ReasonApplyStopped = "apply_stopped"
	// ReasonApplyOwnerUnknown: an apply owns the row but could not be read, so
	// whether the gate is waiting or owed cannot be answered from here.
	ReasonApplyOwnerUnknown = "apply_owner_unknown"
	// ReasonAwaitingPlan: nothing owns the row and no plan result covers the
	// commit yet.
	ReasonAwaitingPlan = "awaiting_plan"
	// ReasonAwaitingReplanAfterApply: an apply settled successfully but its row
	// still names an earlier commit, so the gate holds a result it already has.
	ReasonAwaitingReplanAfterApply = "awaiting_replan_after_apply"
	// ReasonAwaitingParticipantResult: another deployment owes the result this
	// row is folded from.
	ReasonAwaitingParticipantResult = "awaiting_participant_result"
	// ReasonReconciliationOwed: a terminal outcome left the target diverged
	// from the PR. No plan clears this one.
	ReasonReconciliationOwed = "reconciliation_owed"
	// ReasonAggregateRollup: the row is the aggregate. It says nothing of its
	// own; it reports what the per-database rows add up to.
	ReasonAggregateRollup = "aggregate_rollup"
)

// Disposition is the reading of one stored check row against the commit the PR
// is gated on.
type Disposition struct {
	// Reason is the stable code, one of the Reason constants.
	Reason string
	// Summary states what the row says, in one line.
	Summary string
	// Remedy states what moves it, whether that is waiting or acting.
	Remedy string
	// Blocking reports whether this row keeps the aggregate from passing.
	Blocking bool
	// SelfConverging reports whether SchemaBot reaches the resolved state on
	// its own. False means the row is waiting on a person, and how long it has
	// been sitting is not the question to ask about it.
	SelfConverging bool
}

// CoversHead reports whether a stored row speaks for the commit the pull
// request is gated on. An unknown head is not evidence that a row is stale, so
// a row is read as current when no head is known: reporting it as recorded for
// another commit would invent a staleness nothing observed.
//
// Every reader asks this the same way, through here. A caller deriving it from
// a bare string comparison agrees with Diagnose only until the head is unknown,
// and then reports a row as recorded for another commit while that same row's
// disposition says it is current.
func CoversHead(check *storage.Check, headSHA string) bool {
	return headSHA == "" || check.HeadSHA == headSHA
}

// Diagnose reads a stored check row against the commit the PR is gated on.
//
// The row's own commit is what makes this a reading rather than a lookup: a row
// recorded for an earlier commit contributes to the aggregate as blocking
// whatever it concluded, so its stored conclusion alone never answers what the
// PR is waiting for.
//
// apply is the apply the row names, when the caller could read it. Pass nil
// when the row names none, and nil when the lookup failed: an owned row whose
// owner is unreadable is reported as unknown rather than as one more running
// apply, since the owner's state is the whole difference between waiting and
// reconciling.
func Diagnose(check *storage.Check, headSHA string, apply *storage.Apply) Disposition {
	coversHead := CoversHead(check, headSHA)

	// A durable blocking reason outranks everything below it. The writer set it
	// so the block would survive writes that did not re-evaluate the condition,
	// and the publisher honors it the same way, refusing to recompute over it.
	if check.BlockingReason != "" {
		return blockingReasonDisposition(check)
	}
	// The aggregate carries no finding of its own, so reading it as a plan
	// result would attribute a per-database answer to a rollup.
	if IsAggregate(check) {
		return aggregateDisposition(check, coversHead)
	}
	if check.ApplyID != 0 && apply == nil {
		return Disposition{
			Reason:   ReasonApplyOwnerUnknown,
			Summary:  "An apply owns this check, but that apply could not be read from storage.",
			Remedy:   "Find the apply for this database and environment with `sq schemabot status`. Until it is known, treat this row as blocking.",
			Blocking: true,
		}
	}
	if check.Status != StatusCompleted {
		return runningDisposition(check, apply, coversHead)
	}
	if !terminalOutcomeIsSuccess(check) {
		return terminalBlockDisposition(check, coversHead)
	}
	if coversHead {
		return Disposition{
			Reason:         ReasonResolved,
			Summary:        "The plan for this commit concluded successfully.",
			Remedy:         "Nothing to do.",
			SelfConverging: true,
		}
	}
	if check.ApplyID != 0 {
		return Disposition{
			Reason:         ReasonAwaitingReplanAfterApply,
			Summary:        "An apply succeeded, but its result is recorded for an earlier commit, so the gate holds it as blocking.",
			Remedy:         "SchemaBot re-plans this database when the apply settles. If it has not, comment `schemabot plan -e <environment>` on the pull request to record a result for the current commit.",
			Blocking:       true,
			SelfConverging: true,
		}
	}
	return Disposition{
		Reason:         ReasonAwaitingPlan,
		Summary:        "The last plan result is recorded for an earlier commit.",
		Remedy:         "A plan for the current commit is expected. If none arrives, comment `schemabot plan -e <environment>` on the pull request.",
		Blocking:       true,
		SelfConverging: true,
	}
}

// blockingReasonDisposition reads a row a durable guard failed closed. The
// stored reason, not the row's status or ownership, is what says who has to
// move: a rollback clears its apply ownership on the way out, so ownership
// alone would read a reconciliation as an ordinary plan verdict.
func blockingReasonDisposition(check *storage.Check) Disposition {
	switch blockClasses[check.BlockingReason] {
	case blockReconciliation:
		return Disposition{
			Reason:   ReasonReconciliationOwed,
			Summary:  "A terminal outcome left this check blocking, and the target may not match the pull request.",
			Remedy:   "Reconcile the target environment. No plan and no new commit clears this on its own.",
			Blocking: true,
		}
	case blockAwaitingReport:
		return Disposition{
			Reason:         ReasonAwaitingParticipantResult,
			Summary:        "This check is folded from another deployment's result, which has not arrived.",
			Remedy:         "Follow the deployment that owes the result. SchemaBot lifts the block when it reports.",
			Blocking:       true,
			SelfConverging: true,
		}
	case blockGuard:
		fallthrough
	default:
		return Disposition{
			Reason:   ReasonGuardBlocked,
			Summary:  "A guard failed this check closed and recorded why, so a write that did not re-evaluate the guard cannot clear it.",
			Remedy:   "The Check Run's summary states what failed. Fix that, then a new commit or `schemabot plan -e <environment>` re-runs the guard.",
			Blocking: true,
		}
	}
}

// aggregateDisposition reads the rollup row. Its remedy is always the same:
// whatever the per-database rows below it are waiting for.
func aggregateDisposition(check *storage.Check, coversHead bool) Disposition {
	settledPassing := check.Status == StatusCompleted && terminalOutcomeIsSuccess(check)
	if settledPassing && coversHead {
		return Disposition{
			Reason:         ReasonAggregateRollup,
			Summary:        "The rollup passes for this commit.",
			Remedy:         "Nothing to do.",
			SelfConverging: true,
		}
	}
	if !coversHead {
		return Disposition{
			Reason:         ReasonAggregateRollup,
			Summary:        "The rollup is recorded for an earlier commit.",
			Remedy:         "It is republished when a result lands for the current commit. The rows below say what that is waiting on.",
			Blocking:       true,
			SelfConverging: true,
		}
	}
	return Disposition{
		Reason:         ReasonAggregateRollup,
		Summary:        "The rollup is holding the merge gate open.",
		Remedy:         "Read the rows below: the rollup clears when every one of them does.",
		Blocking:       true,
		SelfConverging: true,
	}
}

// runningDisposition reads a row no terminal write has settled yet. An apply
// holding one is not the same as an apply advancing it: a stopped apply keeps
// its check in progress deliberately and settles only when an operator starts
// or cancels it, so waiting is never the answer for one.
func runningDisposition(check *storage.Check, apply *storage.Apply, coversHead bool) Disposition {
	if check.ApplyID == 0 {
		return Disposition{
			Reason:         ReasonAwaitingPlan,
			Summary:        "No plan result has been recorded yet.",
			Remedy:         "A plan is expected. If none arrives, comment `schemabot plan -e <environment>` on the pull request.",
			Blocking:       true,
			SelfConverging: true,
		}
	}
	if applyIsStopped(apply) {
		return Disposition{
			Reason:   ReasonApplyStopped,
			Summary:  "The apply holding this check is stopped, so the check stays in progress and the gate stays closed.",
			Remedy:   "Start the apply or cancel it. It does not settle on its own, and no plan replaces a stopped apply's result.",
			Blocking: true,
		}
	}
	if coversHead {
		return Disposition{
			Reason:         ReasonApplyRunning,
			Summary:        "An apply is running against the commit the pull request is gated on.",
			Remedy:         "Follow it with `sq schemabot status`. The gate clears when the apply settles.",
			Blocking:       true,
			SelfConverging: true,
		}
	}
	return Disposition{
		Reason:         ReasonApplyRunningOnOlderCommit,
		Summary:        "An apply started on an earlier commit is still running, and it owns this check.",
		Remedy:         "A plan cannot replace a running apply's result. The gate clears when the apply settles and a plan for the current commit lands.",
		Blocking:       true,
		SelfConverging: true,
	}
}

// terminalBlockDisposition reads a row a terminal write left blocking without
// recording a durable reason for it, which makes it a plan verdict rather than
// a guard: the Check Run's own summary states the finding.
func terminalBlockDisposition(check *storage.Check, coversHead bool) Disposition {
	if check.ApplyID != 0 {
		return Disposition{
			Reason:   ReasonReconciliationOwed,
			Summary:  "A terminal apply left this check blocking, and the target may not match the pull request.",
			Remedy:   "Reconcile the target environment. No plan and no new commit clears this on its own.",
			Blocking: true,
		}
	}
	if coversHead {
		return Disposition{
			Reason:   ReasonBlocked,
			Summary:  "The plan for this commit concluded blocking.",
			Remedy:   "The Check Run's own summary states what it is blocking on.",
			Blocking: true,
		}
	}
	return Disposition{
		Reason:         ReasonAwaitingPlan,
		Summary:        "The last plan result concluded blocking and is recorded for an earlier commit.",
		Remedy:         "A plan for the current commit is expected. If none arrives, comment `schemabot plan -e <environment>` on the pull request.",
		Blocking:       true,
		SelfConverging: true,
	}
}

// applyIsStopped reports whether the apply holding a check is stopped. An apply
// that could not be read is not treated as stopped; that case is answered
// before this one, as an unknown owner.
func applyIsStopped(apply *storage.Apply) bool {
	return apply != nil && state.IsState(apply.State, state.Apply.Stopped)
}

// terminalOutcomeIsSuccess reports whether a settled row concluded without a
// block. An empty conclusion is not a success: a completed row that names none
// has not recorded an outcome, and reading it as passing would invent one.
//
// This is a narrower question than ConclusionClearsGate, and the two must not
// be collapsed. That one asks what GitHub's branch protection accepts from a
// Check Run, where neutral and skipped pass. This one asks what SchemaBot's own
// aggregate accepts from a stored row, where only success does: a stored row
// concluding neutral is a cancelled apply, and reading it as passing would
// report a row the aggregate holds as blocking as one that clears on its own.
func terminalOutcomeIsSuccess(check *storage.Check) bool {
	return check.Conclusion == ConclusionSuccess
}
