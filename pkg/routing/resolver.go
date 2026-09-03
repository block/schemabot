// Package routing defines the boundary for turning logical SchemaBot
// targets into execution targets.
package routing

import "context"

// Request identifies the logical database/environment a caller wants to target.
// Schema files are intentionally not part of this request: resolving where work
// runs is separate from deciding how namespace-scoped schema changes compose
// into operations.
type Request struct {
	Database    string
	Environment string
}

// ExecutionTarget is one physical/data-plane target returned by a Resolver. A
// single logical request can resolve to multiple targets when an environment
// fans out across deployments. It is not an operation identity: one execution
// target can have multiple concurrent operations.
type ExecutionTarget struct {
	DatabaseType string
	Deployment   string
	Target       string
}

// MemberID is the rollout-member identity of this execution target: the
// deployment it routes through and the target it addresses, together. A
// deployment name alone does not identify a member, because one deployment can
// address several targets; callers that compare, order, or report members must
// use this pair rather than the deployment.
func (t ExecutionTarget) MemberID() string {
	return t.Deployment + "/" + t.Target
}

// DisplayNames returns the operator-facing name of each member of a rollout, in
// the order given. MemberID is the identity every comparison must use, but it is
// not always the right thing to show: a deployment that addresses exactly one
// target is already unambiguous, and naming it "deployment/target" everywhere
// would add a second half that never distinguishes anything. So a member is
// named by its deployment alone unless its deployment addresses more than one
// distinct target in this rollout, in which case every member of that deployment
// is named by its full MemberID.
//
// Keying on distinct targets, rather than on how many members a deployment has,
// is what keeps a keyed or sharded apply — several operations of one deployment
// against the same target — named by the deployment, where the extra half would
// be noise that still did not tell the operations apart.
func DisplayNames(members []ExecutionTarget) []string {
	targetsByDeployment := make(map[string]map[string]struct{}, len(members))
	for _, m := range members {
		if m.Target == "" {
			continue
		}
		if targetsByDeployment[m.Deployment] == nil {
			targetsByDeployment[m.Deployment] = make(map[string]struct{}, 1)
		}
		targetsByDeployment[m.Deployment][m.Target] = struct{}{}
	}

	names := make([]string, len(members))
	for i, m := range members {
		if len(targetsByDeployment[m.Deployment]) > 1 {
			names[i] = m.MemberID()
			continue
		}
		names[i] = m.Deployment
	}
	return names
}

// Resolver resolves logical SchemaBot targets to concrete execution targets.
type Resolver interface {
	ResolveTargets(ctx context.Context, req Request) ([]ExecutionTarget, error)
}
