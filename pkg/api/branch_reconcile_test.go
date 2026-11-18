package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
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

// An elected pass hands each target the configured grace and delete mode, the
// service clock's time, and the environment the target is registered under
// (the reconcile holds that apply target while it deletes), and a database
// that fails does not stop the pass from reconciling the next one.
func TestRunBranchReconcilePassReconcilesEveryTargetOnTheElectedInstance(t *testing.T) {
	reader := reaperMetricReader(t)
	databases := branchReconcileDatabases()
	databases["catalog"] = DatabaseConfig{
		Type:         storage.DatabaseTypeVitess,
		Environments: map[string]EnvironmentConfig{"sandbox": {DSN: "catalog-sandbox-dsn"}},
	}
	svc := newBranchReconcileService(t, BranchReconcileConfig{Enabled: true, Delete: true}, databases, &electingApplyStore{})
	now := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	require.NoError(t, svc.SetClock(clock.NewFake(now)))
	failing := &reconcilingTernClient{err: errors.New("PlanetScale API unavailable")}
	healthy := &reconcilingTernClient{result: tern.BranchReconcileResult{Database: "orders", Candidates: 2, Kept: 1, Orphaned: 1, Deleted: 1}}
	svc.ternClients["catalog/sandbox"] = failing
	svc.ternClients["orders/staging"] = healthy

	svc.runBranchReconcilePass(t.Context(), 36*time.Hour, true)

	assert.Equal(t, []tern.BranchReconcileOptions{{Grace: 36 * time.Hour, Delete: true, Now: now, Environment: "sandbox"}}, failing.calls)
	assert.Equal(t, []tern.BranchReconcileOptions{{Grace: 36 * time.Hour, Delete: true, Now: now, Environment: "staging"}}, healthy.calls,
		"a failed database does not stop the pass")
	assert.Equal(t, []string{"branch_reconcile_error"}, claimFailureReasons(t, reader))
}

// resolvingTernClient is a data plane's default client that resolves targets
// per request; it records each reconcile of its recently applied targets.
type resolvingTernClient struct {
	mockTernClient
	result tern.RecentTargetsReconcileResult
	err    error
	calls  []tern.BranchReconcileOptions
}

func (c *resolvingTernClient) ReconcileRecentTargetBranches(_ context.Context, opts tern.BranchReconcileOptions) (tern.RecentTargetsReconcileResult, error) {
	c.calls = append(c.calls, opts)
	return c.result, c.err
}

// A data plane with no local PlanetScale databases still runs the pass when its
// default client resolves targets per request: the elected instance hands that
// client the configured grace and delete mode and the service clock's time,
// and a failure on it is the pass's outcome.
func TestRunBranchReconcilePassReconcilesRecentlyAppliedTargetsOnADataPlane(t *testing.T) {
	reader := reaperMetricReader(t)
	onlyMySQL := map[string]DatabaseConfig{"payments": branchReconcileDatabases()["payments"]}
	applies := &electingApplyStore{}
	svc := newBranchReconcileService(t, BranchReconcileConfig{Enabled: true, Delete: true}, onlyMySQL, applies)
	now := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	require.NoError(t, svc.SetClock(clock.NewFake(now)))
	router := &resolvingTernClient{result: tern.RecentTargetsReconcileResult{Targets: 2, Deleted: 1}, err: errors.New("target not in the inventory")}
	svc.SetDefaultTernClient(router)

	svc.runBranchReconcilePass(t.Context(), 36*time.Hour, true)

	assert.Equal(t, 1, applies.elections)
	assert.Equal(t, []tern.BranchReconcileOptions{{Grace: 36 * time.Hour, Delete: true, Now: now}}, router.calls)
	assert.Equal(t, []string{"branch_reconcile_error"}, claimFailureReasons(t, reader))
}

// A pass that reconciles every target reports how many branches it deleted in
// total, across the local PlanetScale environments and the targets the data
// plane recently applied to, and records no failure.
func TestRunBranchReconcilePassReportsTheBranchesItDeletedAcrossEveryTarget(t *testing.T) {
	reader := reaperMetricReader(t)
	svc := newBranchReconcileService(t, BranchReconcileConfig{Enabled: true, Delete: true}, branchReconcileDatabases(), &electingApplyStore{})
	var logs bytes.Buffer
	svc.logger = slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	svc.ternClients["orders/staging"] = &reconcilingTernClient{result: tern.BranchReconcileResult{Database: "orders", Candidates: 1, Orphaned: 1, Deleted: 1}}
	svc.SetDefaultTernClient(&resolvingTernClient{result: tern.RecentTargetsReconcileResult{Targets: 3, Deleted: 2, Unreachable: 1}})

	svc.runBranchReconcilePass(t.Context(), 36*time.Hour, true)

	var complete []map[string]any
	for line := range strings.SplitSeq(strings.TrimSpace(logs.String()), "\n") {
		var entry map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &entry), "log line must be JSON: %s", line)
		if entry["msg"] == "branch reconcile pass complete" {
			complete = append(complete, entry)
		}
	}
	require.Len(t, complete, 1, "a pass with no failures logs that it completed; got: %s", logs.String())
	assert.InDelta(t, 3, complete[0]["deleted"], 0, "one local deletion plus two on recently applied targets")
	assert.Empty(t, claimFailureReasons(t, reader), "a target the data plane can no longer reach is not a failure")
}

// Losing the election is the expected outcome on every instance but one, so the
// pass reconciles nothing and records no failure.
func TestRunBranchReconcilePassDoesNothingWhenAnotherInstanceIsElected(t *testing.T) {
	reader := reaperMetricReader(t)
	applies := &electingApplyStore{busy: true}
	svc := newBranchReconcileService(t, BranchReconcileConfig{Enabled: true, Delete: true}, branchReconcileDatabases(), applies)
	client := &reconcilingTernClient{}
	svc.ternClients["orders/staging"] = client

	svc.runBranchReconcilePass(t.Context(), time.Hour, true)

	assert.Equal(t, 1, applies.elections)
	assert.Empty(t, client.calls)
	assert.Empty(t, claimFailureReasons(t, reader))
}

// The pass starts whenever it is enabled with a valid grace period, and the
// configured delete mode and default grace reach the reconcile.
func TestBranchReconcilePassStartsOnlyWhenEnabledWithAValidGrace(t *testing.T) {
	tests := []struct {
		name   string
		cfg    BranchReconcileConfig
		starts bool
	}{
		{name: "disabled", cfg: BranchReconcileConfig{}},
		{name: "invalid grace", cfg: BranchReconcileConfig{Enabled: true, Grace: "soon"}},
		{name: "enabled", cfg: BranchReconcileConfig{Enabled: true}, starts: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := newBranchReconcileService(t, tt.cfg, branchReconcileDatabases(), &electingApplyStore{})
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

// A local server can register a PlanetScale database while it runs. The pass
// reads its targets on every tick, so a server that started with none skips
// the election until one is registered and then reconciles it.
func TestBranchReconcilePassPicksUpADatabaseRegisteredAfterStartup(t *testing.T) {
	onlyMySQL := map[string]DatabaseConfig{"payments": branchReconcileDatabases()["payments"]}
	applies := &electingApplyStore{}
	svc := newBranchReconcileService(t, BranchReconcileConfig{Enabled: true}, onlyMySQL, applies)
	svc.config.EnableLocalRegistration()
	client := &reconcilingTernClient{}
	svc.ternClients["orders/staging"] = client

	pass, ok := svc.branchReconcilePass()
	require.True(t, ok, "an enabled pass starts before any PlanetScale database is registered")
	pass(t.Context())
	assert.Zero(t, applies.elections, "with no targets the pass does not contend for the election")

	next := *svc.config
	next.Databases = map[string]DatabaseConfig{
		"payments": onlyMySQL["payments"],
		"orders": {
			Type:         storage.DatabaseTypeVitess,
			Environments: map[string]EnvironmentConfig{"staging": {DSN: "orders-staging-dsn"}},
		},
	}
	_, publish, err := svc.config.PrepareLocalRegistration(&next, map[string]map[string]string{"orders": {"staging": "orders-staging-dsn"}})
	require.NoError(t, err)
	publish()

	pass(t.Context())
	require.Len(t, client.calls, 1)
	assert.Equal(t, "staging", client.calls[0].Environment)
}

// electionSignallingApplyStore reports each branch reconcile election on a
// channel and declines it, so a test can see that the pass ran.
type electionSignallingApplyStore struct {
	*capturingApplyStore
	once sync.Once
	ran  chan struct{}
}

func (s *electionSignallingApplyStore) WithBranchReconcileElection(context.Context, func(context.Context) error) error {
	s.once.Do(func() { close(s.ran) })
	return storage.ErrBranchReconcileBusy
}

// Starting the operator starts the branch reconcile pass when it is enabled.
func TestStartOperatorStartsTheBranchReconcilePass(t *testing.T) {
	store := &electionSignallingApplyStore{capturingApplyStore: &capturingApplyStore{}, ran: make(chan struct{})}
	svc, _ := newQueueApplyTestService(trustedQueueApplyTestPlan(), &mockTernClient{}, store)
	svc.config.BranchReconcile = BranchReconcileConfig{Enabled: true}
	svc.config.Databases["orders"] = DatabaseConfig{
		Type:         storage.DatabaseTypeVitess,
		Environments: map[string]EnvironmentConfig{"staging": {DSN: "orders-staging-dsn"}},
	}
	require.NoError(t, svc.SetOperatorPollInterval(time.Hour))

	svc.StartOperator(t.Context())
	defer svc.StopOperator()

	select {
	case <-store.ran:
	case <-time.After(10 * time.Second):
		t.Fatal("the operator never ran the branch reconcile pass")
	}
}
