// Package inventory resolves opaque data-plane targets into inventory records.
package inventory

import "context"

// Request identifies the opaque execution target a data plane must look up.
// For inventory-backed deployments, Target is typically a DSID. Static
// deployments can use any stable target key. The requested database namespace
// stays on the Tern request and is not part of inventory resolution.
type Request struct {
	Target       string
	DatabaseType string
	Environment  string
}

// Target is the resolved inventory record for a target.
type Target struct {
	Target       string
	DatabaseType string
	DSN          string
	Metadata     map[string]string
}

// Resolver resolves opaque execution targets to inventory records.
type Resolver interface {
	ResolveTarget(ctx context.Context, req Request) (*Target, error)
}
