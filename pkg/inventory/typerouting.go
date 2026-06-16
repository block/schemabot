package inventory

import (
	"context"
	"fmt"
)

// TypeRoutingResolver routes a request to the Resolver registered for its
// database type. Each engine resolver (for example an Etre resolver bound to one
// entity type and connection assembler) serves a single engine, so a data plane
// that serves more than one engine composes one per type behind a single
// Resolver and lets the request's database type select among them.
type TypeRoutingResolver struct {
	byType map[string]Resolver
}

var _ Resolver = (*TypeRoutingResolver)(nil)

// NewTypeRoutingResolver builds a resolver that dispatches by database type. The
// keys are database types (for example "mysql", "vitess"); each value resolves
// targets for that type. It fails closed on an empty set, an empty type key, or
// a nil resolver so a misconfiguration surfaces at construction, not on the
// first request.
func NewTypeRoutingResolver(byType map[string]Resolver) (*TypeRoutingResolver, error) {
	if len(byType) == 0 {
		return nil, fmt.Errorf("at least one resolver is required")
	}
	resolvers := make(map[string]Resolver, len(byType))
	for databaseType, resolver := range byType {
		if databaseType == "" {
			return nil, fmt.Errorf("database type key must not be empty")
		}
		if resolver == nil {
			return nil, fmt.Errorf("resolver for database type %q must not be nil", databaseType)
		}
		resolvers[databaseType] = resolver
	}
	return &TypeRoutingResolver{byType: resolvers}, nil
}

// ResolveTarget dispatches to the resolver registered for the request's database
// type. It fails closed when the request carries no type or no resolver is
// registered for it, rather than guessing an engine.
func (r *TypeRoutingResolver) ResolveTarget(ctx context.Context, req Request) (*Target, error) {
	if req.DatabaseType == "" {
		return nil, fmt.Errorf("resolve target %q: a database type is required to select an engine resolver", req.Target)
	}
	resolver, ok := r.byType[req.DatabaseType]
	if !ok {
		return nil, fmt.Errorf("resolve target %q: no resolver registered for database type %q", req.Target, req.DatabaseType)
	}
	return resolver.ResolveTarget(ctx, req)
}
