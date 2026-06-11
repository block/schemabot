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

// Resolver resolves logical SchemaBot targets to concrete execution targets.
type Resolver interface {
	ResolveTargets(ctx context.Context, req Request) ([]ExecutionTarget, error)
}
