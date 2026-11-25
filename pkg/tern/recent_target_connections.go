package tern

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/inventory"
	"github.com/block/schemabot/pkg/storage"
)

// RecentTargetConnection is one target this data plane recently applied to,
// resolved to the connection an apply on it uses.
type RecentTargetConnection struct {
	// Target is the resolver key the applies were routed by.
	Target string
	// Environment is the environment the applies were stored with.
	Environment string
	// DSN is the target's resolved connection string. It carries a credential,
	// so it must never be logged.
	DSN string
	// Metadata is the resolved record's metadata. For some database types
	// part of how a target is dialed lives here and not in the DSN, such as
	// the CA a PostgreSQL target is verified against, so a caller that opens
	// the target passes it on to connect under the trust an apply uses. It
	// can carry credentials, so it must never be logged.
	Metadata map[string]string
}

// RecentTargetConnections is what one listing of recently applied targets
// found.
type RecentTargetConnections struct {
	// Connections holds one entry per target and environment that resolved,
	// ordered by target and then environment.
	Connections []RecentTargetConnection
	// Unreachable is how many targets the inventory no longer has. They are
	// left out of Connections and are not counted as failures.
	Unreachable int
	// Unresolved is how many targets could not be resolved for any other
	// reason. They are left out of Connections, and the returned error says
	// why for each.
	Unresolved int
}

// RecentTargetConnections returns the connection of every target of
// databaseType this data plane's applies were written for within lookback.
//
// A data plane that resolves targets per request has no static list of
// databases, so the targets come from its own storage: each distinct target
// and environment its apply operations ran against. Several databases can
// live on one target, and a connection reaches the whole target, so a target
// is listed once per environment however many databases were applied to on
// it. Each is resolved the way an apply on it is resolved, so the caller
// reaches the target with the credential a drive would use now.
//
// A target that cannot be resolved is logged and the listing moves on; the
// failures are joined and returned with the connections that did resolve, so
// a caller can work on those and report the rest. A target the inventory
// answered for and no longer has is logged at warn and is not a failure: it
// would otherwise fail every listing until it left the window. When the
// targets cannot be listed at all, the error is returned with no connections:
// what this data plane applied to is then unknown, not empty.
func (r *TargetRouter) RecentTargetConnections(ctx context.Context, databaseType string, lookback time.Duration) (RecentTargetConnections, error) {
	applied, err := r.storage.ApplyOperations().ListRecentTargets(ctx, databaseType, lookback)
	if err != nil {
		return RecentTargetConnections{}, fmt.Errorf("list %s targets applied to within %s: %w", databaseType, lookback, err)
	}

	var result RecentTargetConnections
	var errs []error
	for _, target := range distinctTargetEnvironments(applied) {
		resolved, err := r.resolveConnectableTarget(ctx, target.Target, databaseType, target.Environment)
		switch {
		case errors.Is(err, inventory.ErrTargetNotFound):
			result.Unreachable++
			r.logger.Warn("a recently applied target is no longer in the inventory; it is left out of the recently applied targets and nothing reaches it from here",
				"database_type", databaseType, "environment", target.Environment, "target", target.Target, "error", err)
		case err != nil:
			result.Unresolved++
			r.logger.Error("a recently applied target could not be resolved; it is left out of the recently applied targets until it resolves",
				"database_type", databaseType, "environment", target.Environment, "target", target.Target, "error", err)
			errs = append(errs, fmt.Errorf("resolve %s target %q in environment %q: %w", databaseType, target.Target, target.Environment, err))
		case resolved.DSN == "":
			// A type that is reached without a DSN resolves as connectable and
			// still has no connection string to hand back.
			result.Unresolved++
			r.logger.Error("a recently applied target resolved without a connection string; it is left out of the recently applied targets",
				"database_type", databaseType, "environment", target.Environment, "target", target.Target)
			errs = append(errs, fmt.Errorf("resolve %s target %q in environment %q: the inventory record has no DSN", databaseType, target.Target, target.Environment))
		default:
			result.Connections = append(result.Connections, RecentTargetConnection{
				Target:      target.Target,
				Environment: target.Environment,
				DSN:         resolved.DSN,
				Metadata:    maps.Clone(resolved.Metadata),
			})
		}
	}
	return result, errors.Join(errs...)
}

// targetEnvironment is one target under one of the environments its applies
// were stored with.
type targetEnvironment struct {
	Target      string
	Environment string
}

// distinctTargetEnvironments returns each target and environment in applied
// once, ordered by target and then environment. The environment stays part of
// the identity because a resolver that scopes by environment resolves the
// same target key differently under each.
func distinctTargetEnvironments(applied []storage.AppliedTarget) []targetEnvironment {
	seen := make(map[targetEnvironment]struct{}, len(applied))
	distinct := make([]targetEnvironment, 0, len(applied))
	for _, target := range applied {
		key := targetEnvironment{Target: target.Target, Environment: target.Environment}
		if _, listed := seen[key]; !listed {
			seen[key] = struct{}{}
			distinct = append(distinct, key)
		}
	}
	slices.SortFunc(distinct, func(a, b targetEnvironment) int {
		return cmp.Or(strings.Compare(a.Target, b.Target), strings.Compare(a.Environment, b.Environment))
	})
	return distinct
}
