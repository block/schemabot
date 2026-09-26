package api

import (
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
