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

// MultiTargetDeployments reports which deployments of a member set address more
// than one distinct target.
//
// It is the point at which a deployment name stops identifying one member, and
// so the one place anything qualifies itself with the target: the name an
// operator reads, and the key an operation is stored under. Deriving both from
// this keeps them from disagreeing about when a target is worth naming.
//
// Distinct targets rather than member count is the test on purpose. A keyed or
// sharded apply has several members on one target, and naming the target there
// would add a component that still does not tell them apart.
func MultiTargetDeployments(members []ExecutionTarget) map[string]bool {
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
	multi := make(map[string]bool, len(targetsByDeployment))
	for deployment, targets := range targetsByDeployment {
		if len(targets) > 1 {
			multi[deployment] = true
		}
	}
	return multi
}

// Resolver resolves logical SchemaBot targets to concrete execution targets.
type Resolver interface {
	ResolveTargets(ctx context.Context, req Request) ([]ExecutionTarget, error)
}
