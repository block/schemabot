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
	// SchemaOverrides maps a requested (canonical) MySQL namespace to the
	// physical schema name on this target. When non-empty it is a strict
	// allowlist: a requested namespace without a mapping fails rather than
	// falling back to the canonical name. Empty preserves the default
	// behavior where the requested namespace is the physical schema.
	SchemaOverrides map[string]string
}

// Resolver resolves opaque execution targets to inventory records.
type Resolver interface {
	ResolveTarget(ctx context.Context, req Request) (*Target, error)
}

// ProbeRequest identifies an inventory target that can be checked without an
// incoming operation request. It carries every field a Request needs to
// resolve the same target again: Environment is set when the resolver refuses
// to resolve without one, and stays empty for resolvers that do not scope by
// environment.
type ProbeRequest struct {
	Target       string
	DatabaseType string
	Environment  string
}

// Enumerator is an optional capability for resolvers that can list their
// targets without an incoming operation request. Listing may need I/O, since
// a discovery-backed inventory holds its target set remotely, so it takes a
// context and can fail; a failed enumeration means coverage is unknown, not
// empty, and callers must not treat it as an empty list.
type Enumerator interface {
	// Enumerate returns one request per target in a deterministic order.
	Enumerate(ctx context.Context) ([]ProbeRequest, error)
	// UnenumerableDatabaseTypes reports the database types this resolver
	// serves whose targets it cannot list, so a caller can say which part of
	// the inventory an enumeration does not cover. A resolver that lists every
	// target it serves returns nil.
	UnenumerableDatabaseTypes() []string
}
