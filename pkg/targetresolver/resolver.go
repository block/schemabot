// Package targetresolver defines the boundary for turning logical SchemaBot
// targets into execution targets.
package targetresolver

import "context"

// Request identifies the logical database/environment a caller wants to target.
// Schema files are intentionally not part of this request: resolving where work
// runs is separate from deciding how namespace-scoped schema changes compose
// into operations.
type Request struct {
	Database    string
	Environment string
}

// Target is one execution target returned by a Resolver. A single logical
// request can resolve to multiple targets when an environment fans out across
// deployments.
type Target struct {
	DatabaseType string
	Deployment   string
	Target       string
}

// Resolver resolves logical SchemaBot targets to concrete execution targets.
type Resolver interface {
	ResolveTargets(ctx context.Context, req Request) ([]Target, error)
}
