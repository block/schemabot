package api

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/clock"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// reconcilingTernClient is a local PlanetScale tern client that records each
// reconcile it is asked for and answers with a canned result.
type reconcilingTernClient struct {
	mockTernClient
	result tern.BranchReconcileResult
	err    error
	calls  []tern.BranchReconcileOptions
}

func (c *reconcilingTernClient) ReconcileOrphanedBranches(_ context.Context, opts tern.BranchReconcileOptions) (tern.BranchReconcileResult, error) {
	c.calls = append(c.calls, opts)
	return c.result, c.err
}

// electingApplyStore runs the reconcile when this instance wins the election
// and reports the lock held elsewhere when it does not.
type electingApplyStore struct {
	storage.ApplyStore
	busy      bool
	elections int
}

func (s *electingApplyStore) WithBranchReconcileElection(ctx context.Context, fn func(context.Context) error) error {
	s.elections++
	if s.busy {
		return storage.ErrBranchReconcileBusy
	}
	return fn(ctx)
}

// branchReconcileDatabases registers the PlanetScale database orders with a
// local staging environment and a production environment routed to another
// deployment, and the MySQL database payments with a local DSN.
func branchReconcileDatabases() map[string]DatabaseConfig {
	return map[string]DatabaseConfig{
		"orders": {
			Type: storage.DatabaseTypeVitess,
			Environments: map[string]EnvironmentConfig{
				"staging":    {DSN: "orders-staging-dsn"},
				"production": {Target: "orders-production", Deployment: "eu"},
			},
		},
		"payments": {
			Type: storage.DatabaseTypeMySQL,
			Environments: map[string]EnvironmentConfig{
				"staging": {DSN: "payments-staging-dsn"},
			},
		},
	}
}

func newBranchReconcileService(t *testing.T, cfg BranchReconcileConfig, databases map[string]DatabaseConfig, applies *electingApplyStore) *Service {
	t.Helper()
	svc := New(&mockStorageWithApplyStores{applies: applies}, &ServerConfig{Databases: databases, BranchReconcile: cfg},
		nil, slog.New(slog.DiscardHandler))
	return svc
}

// Only a PlanetScale database this process executes against itself is
// reconciled: a routed environment belongs to the deployment it routes to,
// and a MySQL database has no branches.
func TestBranchReconcileTargetsAreLocalPlanetScaleEnvironments(t *testing.T) {
	databases := branchReconcileDatabases()
	databases["catalog"] = DatabaseConfig{
		Type:         storage.DatabaseTypeVitess,
		Environments: map[string]EnvironmentConfig{"staging": {DSN: "catalog-staging-dsn"}},
	}
	svc := newBranchReconcileService(t, BranchReconcileConfig{Enabled: true}, databases, &electingApplyStore{})

	assert.Equal(t, []branchReconcileTarget{
		{database: "catalog", environment: "staging"},
		{database: "orders", environment: "staging"},
	}, svc.branchReconcileTargets())
}

// An elected pass hands each target the configured grace and delete mode and
// the service clock's time, and a database that fails does not stop the pass
// from reconciling the next one.
func TestRunBranchReconcilePassReconcilesEveryTargetOnTheElectedInstance(t *testing.T) {
	reader := reaperMetricReader(t)
	applies := &electingApplyStore{}
	svc := newBranchReconcileService(t, BranchReconcileConfig{Enabled: true, Delete: true}, branchReconcileDatabases(), applies)
	now := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	require.NoError(t, svc.SetClock(clock.NewFake(now)))
	failing := &reconcilingTernClient{err: errors.New("PlanetScale API unavailable")}
	healthy := &reconcilingTernClient{result: tern.BranchReconcileResult{Database: "orders", Candidates: 2, Kept: 1, Orphaned: 1, Deleted: 1}}
	svc.ternClients["catalog/staging"] = failing
	svc.ternClients["orders/staging"] = healthy
	targets := []branchReconcileTarget{{database: "catalog", environment: "staging"}, {database: "orders", environment: "staging"}}

	svc.runBranchReconcilePass(t.Context(), targets, 36*time.Hour, true)

	want := tern.BranchReconcileOptions{Grace: 36 * time.Hour, Delete: true, Now: now}
	assert.Equal(t, []tern.BranchReconcileOptions{want}, failing.calls)
	assert.Equal(t, []tern.BranchReconcileOptions{want}, healthy.calls, "a failed database does not stop the pass")
	assert.Equal(t, []string{"branch_reconcile_error"}, claimFailureReasons(t, reader))
}

// Losing the election is the expected outcome on every instance but one, so the
// pass reconciles nothing and records no failure.
func TestRunBranchReconcilePassDoesNothingWhenAnotherInstanceIsElected(t *testing.T) {
	reader := reaperMetricReader(t)
	applies := &electingApplyStore{busy: true}
	svc := newBranchReconcileService(t, BranchReconcileConfig{Enabled: true, Delete: true}, branchReconcileDatabases(), applies)
	client := &reconcilingTernClient{}
	svc.ternClients["orders/staging"] = client

	svc.runBranchReconcilePass(t.Context(), []branchReconcileTarget{{database: "orders", environment: "staging"}}, time.Hour, true)

	assert.Equal(t, 1, applies.elections)
	assert.Empty(t, client.calls)
	assert.Empty(t, claimFailureReasons(t, reader))
}

// The pass starts only when it is enabled, its grace period is valid, and this
// process executes against at least one PlanetScale database; the configured
// delete mode reaches the reconcile.
func TestBranchReconcilePassStartsOnlyWhenItHasWorkToDo(t *testing.T) {
	onlyMySQL := map[string]DatabaseConfig{"payments": branchReconcileDatabases()["payments"]}
	tests := []struct {
		name      string
		cfg       BranchReconcileConfig
		databases map[string]DatabaseConfig
		starts    bool
	}{
		{name: "disabled", cfg: BranchReconcileConfig{}, databases: branchReconcileDatabases()},
		{name: "invalid grace", cfg: BranchReconcileConfig{Enabled: true, Grace: "soon"}, databases: branchReconcileDatabases()},
		{name: "no local PlanetScale database", cfg: BranchReconcileConfig{Enabled: true}, databases: onlyMySQL},
		{name: "enabled with a local PlanetScale database", cfg: BranchReconcileConfig{Enabled: true}, databases: branchReconcileDatabases(), starts: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := newBranchReconcileService(t, tt.cfg, tt.databases, &electingApplyStore{})
			client := &reconcilingTernClient{}
			svc.ternClients["orders/staging"] = client

			pass, ok := svc.branchReconcilePass()

			require.Equal(t, tt.starts, ok)
			if !ok {
				return
			}
			pass(t.Context())
			require.Len(t, client.calls, 1)
			assert.Equal(t, DefaultBranchReconcileGrace, client.calls[0].Grace)
			assert.False(t, client.calls[0].Delete, "an enabled pass only reports unless delete is set")
		})
	}
}
