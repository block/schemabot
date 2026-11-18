package api

import (
	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/routing"
)

// MembersWithWork returns the rollout members, primary included, whose plan in
// this rollup would change something, in rollout order.
//
// An errored member is never counted: it has no plan to read work from. That is
// not the same as having none, and a rollup carrying one is not Clean, so a
// caller deciding whether there is anything to apply must refuse a rollup that is
// not Clean rather than read an empty result from it as "nothing to do".
func (r PlanRollup) MembersWithWork() []routing.ExecutionTarget {
	var out []routing.ExecutionTarget
	for _, entry := range r.Entries {
		if entry.Class == DeploymentErrored || !entry.ChangeSet.HasWork() {
			continue
		}
		out = append(out, routing.ExecutionTarget{Deployment: entry.Deployment, Target: entry.Target})
	}
	return out
}

// MemberCopiesUnreported finds the first rollout member after the primary
// whose plan has work and whose data plane did not report whether applying it
// discards an unfinished copy. It returns that member's index in Entries and
// why, or -1 and "" when every such member reported.
//
// Such a member refuses the apply: the consent to destroy a copy is given
// against a disclosure, and a member that disclosed nothing cannot be shown to
// have nothing to disclose. The primary is skipped: its copies are disclosed
// on the primary plan itself.
func (r PlanRollup) MemberCopiesUnreported() (int, string) {
	for i, entry := range r.Entries {
		if i == 0 || entry.Class == DeploymentErrored || !entry.ChangeSet.HasWork() {
			continue
		}
		if !entry.ExistingCopiesReported {
			return i, "its data plane did not report whether applying its plan discards an unfinished copy"
		}
	}
	return -1, ""
}

// MemberDiscardedCopy is an unfinished copy on a rollout member after the
// primary that applying the member's plan discards.
type MemberDiscardedCopy struct {
	// Entry is the member's index in the rollup's Entries.
	Entry int
	// Copy is the copy, as the member's data plane reported it.
	Copy *apitypes.ExistingCopyResponse
}

// MemberDiscardedCopies lists the unfinished copies applying the rollout's
// member plans discards, after the primary, in rollout order. The primary's
// own are on the primary plan. Every copy the member did not report as
// adopted counts, so a disposition this build does not recognize is disclosed
// as a discard rather than passed over.
func (r PlanRollup) MemberDiscardedCopies() []MemberDiscardedCopy {
	var out []MemberDiscardedCopy
	for i, entry := range r.Entries {
		if i == 0 || entry.Class == DeploymentErrored || !entry.ChangeSet.HasWork() {
			continue
		}
		for _, existing := range entry.ExistingCopies {
			if existing == nil || existing.GetDisposition() == string(engine.CopyAdopt) {
				continue
			}
			out = append(out, MemberDiscardedCopy{Entry: i, Copy: existingCopiesFromProto([]*ternv1.ExistingCopy{existing})[0]})
		}
	}
	return out
}
