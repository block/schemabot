package tern

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/inventory"
	"github.com/block/schemabot/pkg/storage"
)

// recentTargetsOperationStore serves a fixed list of recently applied targets
// per database type, or err when it is set, and records what it was asked for.
type recentTargetsOperationStore struct {
	storage.ApplyOperationStore
	recent map[string][]storage.AppliedTarget
	err    error
	asked  []string
}

func (s *recentTargetsOperationStore) ListRecentTargets(_ context.Context, databaseType string, lookback time.Duration) ([]storage.AppliedTarget, error) {
	s.asked = append(s.asked, databaseType+" within "+lookback.String())
	if s.err != nil {
		return nil, s.err
	}
	return s.recent[databaseType], nil
}

// recentTargetsStorage is a storage whose only working store is the apply
// operations.
type recentTargetsStorage struct {
	storage.Storage
	operations *recentTargetsOperationStore
}

func (s *recentTargetsStorage) ApplyOperations() storage.ApplyOperationStore { return s.operations }

// recordingDSNResolver resolves every target to a MySQL record whose DSN names
// the target and the environment it was asked under, and records each request.
type recordingDSNResolver struct {
	requests []inventory.Request
	failing  map[string]error
	noDSN    map[string]bool
}

func (r *recordingDSNResolver) ResolveTarget(_ context.Context, req inventory.Request) (*inventory.Target, error) {
	r.requests = append(r.requests, req)
	if err := r.failing[req.Target]; err != nil {
		return nil, err
	}
	resolved := &inventory.Target{Target: req.Target, DatabaseType: req.DatabaseType}
	if !r.noDSN[req.Target] {
		resolved.DSN = fmt.Sprintf("ddl:secret@tcp(%s.%s.db.test:3306)/", req.Target, req.Environment)
	}
	return resolved, nil
}

func newRecentTargetsRouter(t *testing.T, resolver inventory.Resolver, operations *recentTargetsOperationStore) *TargetRouter {
	t.Helper()
	router, err := NewTargetRouter(TargetRouterConfig{
		Resolver: resolver,
		Storage:  &recentTargetsStorage{operations: operations},
		Logger:   slog.New(slog.DiscardHandler),
		LocalClientFactory: func(LocalConfig, storage.Storage, *slog.Logger) (Client, error) {
			return nil, errors.New("listing recently applied targets builds no client")
		},
	})
	require.NoError(t, err)
	return router
}

// A data plane applied to four databases on three MySQL targets, one of them
// in two environments and one shared by two databases, and to a PlanetScale
// target. The listing for MySQL returns one connection per MySQL target and
// environment, ordered by target and then environment, each resolved under the
// environment its applies were stored with, and leaves the PlanetScale target
// out.
func TestRecentTargetConnectionsListsEachTargetOncePerEnvironment(t *testing.T) {
	operations := &recentTargetsOperationStore{recent: map[string][]storage.AppliedTarget{
		storage.DatabaseTypeMySQL: {
			{Database: "ledger", Environment: "staging", Target: "payments-cluster"},
			{Database: "orders", Environment: "production", Target: "orders-cluster"},
			{Database: "orders", Environment: "staging", Target: "orders-cluster"},
			{Database: "refunds", Environment: "staging", Target: "payments-cluster"},
			{Database: "taxes", Environment: "staging", Target: "billing-cluster"},
		},
		storage.DatabaseTypeVitess: {
			{Database: "catalog", Environment: "production", Target: "catalog-keyspace"},
		},
	}}
	resolver := &recordingDSNResolver{}
	router := newRecentTargetsRouter(t, resolver, operations)

	result, err := router.RecentTargetConnections(t.Context(), storage.DatabaseTypeMySQL, 37*24*time.Hour)

	require.NoError(t, err)
	assert.Equal(t, RecentTargetConnections{Connections: []RecentTargetConnection{
		{Target: "billing-cluster", Environment: "staging", DSN: "ddl:secret@tcp(billing-cluster.staging.db.test:3306)/"},
		{Target: "orders-cluster", Environment: "production", DSN: "ddl:secret@tcp(orders-cluster.production.db.test:3306)/"},
		{Target: "orders-cluster", Environment: "staging", DSN: "ddl:secret@tcp(orders-cluster.staging.db.test:3306)/"},
		{Target: "payments-cluster", Environment: "staging", DSN: "ddl:secret@tcp(payments-cluster.staging.db.test:3306)/"},
	}}, result)
	assert.Equal(t, []string{"mysql within 888h0m0s"}, operations.asked)
	assert.Equal(t, []inventory.Request{
		{Target: "billing-cluster", DatabaseType: storage.DatabaseTypeMySQL, Environment: "staging"},
		{Target: "orders-cluster", DatabaseType: storage.DatabaseTypeMySQL, Environment: "production"},
		{Target: "orders-cluster", DatabaseType: storage.DatabaseTypeMySQL, Environment: "staging"},
		{Target: "payments-cluster", DatabaseType: storage.DatabaseTypeMySQL, Environment: "staging"},
	}, resolver.requests, "a target two databases share is resolved once")
}

// A target the inventory no longer has is counted and left out without failing
// the listing. A target the inventory could not answer for, and one that
// resolved with no connection string, are failures: each is counted and named
// in the error, and the target that did resolve is still returned.
func TestRecentTargetConnectionsReportsTheTargetsItCannotResolve(t *testing.T) {
	operations := &recentTargetsOperationStore{recent: map[string][]storage.AppliedTarget{
		storage.DatabaseTypeMySQL: {
			{Database: "ledger", Environment: "production", Target: "no-dsn-cluster"},
			{Database: "orders", Environment: "production", Target: "orders-cluster"},
			{Database: "old", Environment: "production", Target: "retired-cluster"},
			{Database: "users", Environment: "production", Target: "unreadable-cluster"},
		},
	}}
	resolver := &recordingDSNResolver{
		failing: map[string]error{
			"retired-cluster":    fmt.Errorf("target %q is not configured: %w", "retired-cluster", inventory.ErrTargetNotFound),
			"unreadable-cluster": errors.New("inventory unavailable"),
		},
		noDSN: map[string]bool{"no-dsn-cluster": true},
	}
	router := newRecentTargetsRouter(t, resolver, operations)

	result, err := router.RecentTargetConnections(t.Context(), storage.DatabaseTypeMySQL, time.Hour)

	require.Error(t, err)
	assert.ErrorContains(t, err, `resolve mysql target "unreadable-cluster" in environment "production": inventory unavailable`)
	assert.ErrorContains(t, err, `resolver returned an incomplete target for "no-dsn-cluster" (type="mysql"): missing DSN`)
	assert.NotErrorIs(t, err, inventory.ErrTargetNotFound, "a target the inventory no longer has is not a failure")
	assert.Equal(t, RecentTargetConnections{
		Connections: []RecentTargetConnection{
			{Target: "orders-cluster", Environment: "production", DSN: "ddl:secret@tcp(orders-cluster.production.db.test:3306)/"},
		},
		Unreachable: 1,
		Unresolved:  2,
	}, result)
}

// A type that is reached without a DSN resolves as connectable and has no
// connection string, so its targets are failures, not empty connections.
func TestRecentTargetConnectionsRefusesATargetReachedWithoutADSN(t *testing.T) {
	operations := &recentTargetsOperationStore{recent: map[string][]storage.AppliedTarget{
		storage.DatabaseTypeVitess: {{Database: "catalog", Environment: "production", Target: "catalog-keyspace"}},
	}}
	router := newRecentTargetsRouter(t, planetScaleTargetResolver(), operations)

	result, err := router.RecentTargetConnections(t.Context(), storage.DatabaseTypeVitess, time.Hour)

	require.ErrorContains(t, err, `resolve vitess target "catalog-keyspace" in environment "production": the inventory record has no DSN`)
	assert.Equal(t, RecentTargetConnections{Unresolved: 1}, result)
}

// When storage cannot list the targets, no target is resolved and the error
// says what was being read: what the data plane applied to is unknown.
func TestRecentTargetConnectionsFailsWhenTheTargetsCannotBeListed(t *testing.T) {
	listErr := errors.New("storage unavailable")
	resolver := &recordingDSNResolver{}
	router := newRecentTargetsRouter(t, resolver, &recentTargetsOperationStore{err: listErr})

	result, err := router.RecentTargetConnections(t.Context(), storage.DatabaseTypeMySQL, 2*time.Hour)

	require.ErrorIs(t, err, listErr)
	assert.ErrorContains(t, err, "list mysql targets applied to within 2h0m0s")
	assert.Equal(t, RecentTargetConnections{}, result)
	assert.Empty(t, resolver.requests)
}
