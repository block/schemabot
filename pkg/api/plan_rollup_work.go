package api

import (
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/engine"
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

// MemberCopyAtStake finds the first rollout member after the primary whose plan
// has work and whose apply is not known to leave the unfinished copies on its
// target intact: its diff discards a copy, or its data plane did not say. It
// returns that member's index in Entries and why, or -1 and "" when there is
// none.
//
// A data plane that did not report copies counts as putting one at stake,
// because the consent to destroy a copy is given against a disclosure, and a
// member that disclosed nothing cannot be shown to have nothing to disclose.
// The primary is skipped: its copies are disclosed on the primary plan itself.
func (r PlanRollup) MemberCopyAtStake() (int, string) {
	for i, entry := range r.Entries {
		if i == 0 || entry.Class == DeploymentErrored || !entry.ChangeSet.HasWork() {
			continue
		}
		if !entry.ExistingCopiesReported {
			return i, "its data plane did not report whether applying its plan discards an unfinished copy"
		}
		for _, existing := range entry.ExistingCopies {
			if existing == nil || existing.GetDisposition() == string(engine.CopyAdopt) {
				continue
			}
			return i, fmt.Sprintf("applying its plan discards the unfinished copy of %s in namespace %q",
				strings.Join(existing.GetTables(), ", "), existing.GetNamespace())
		}
	}
	return -1, ""
}
