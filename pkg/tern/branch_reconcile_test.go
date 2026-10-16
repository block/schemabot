package tern

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"testing"
	"time"

	ps "github.com/planetscale/planetscale-go/planetscale"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine/planetscale"
	"github.com/block/schemabot/pkg/inventory"
	"github.com/block/schemabot/pkg/psclient"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// branchReconcileClient serves a fixed branch listing and the open deploy
// requests per branch, and records every delete it is sent.
type branchReconcileClient struct {
	psclient.PSClient
	branches   []*ps.DatabaseBranch
	openDRs    map[string][]*ps.DeployRequest
	drErr      map[string]error
	deleteErr  error
	deleted    []string
	drRequests []*ps.ListDeployRequestsRequest
	onDelete   func()
}

func (c *branchReconcileClient) ListBranches(context.Context, *ps.ListDatabaseBranchesRequest) ([]*ps.DatabaseBranch, error) {
	return c.branches, nil
}

func (c *branchReconcileClient) ListDeployRequests(_ context.Context, req *ps.ListDeployRequestsRequest) ([]*ps.DeployRequest, error) {
	c.drRequests = append(c.drRequests, req)
	if err := c.drErr[req.Branch]; err != nil {
		return nil, err
	}
	return c.openDRs[req.Branch], nil
}

func (c *branchReconcileClient) DeleteBranch(_ context.Context, req *ps.DeleteDatabaseBranchRequest) error {
	if c.onDelete != nil {
		c.onDelete()
	}
	if c.deleteErr != nil {
		return c.deleteErr
	}
	c.deleted = append(c.deleted, req.Branch)
	return nil
}

// branchReconcileApplyStore serves the unsettled applies and holds the apply
// target. When startedLater is set, every read after the first returns it too,
// as if those applies started while the pass ran; when errLater is set, every
// read after the first fails with it; when holdErr is set, the target cannot
// be held.
type branchReconcileApplyStore struct {
	storage.ApplyStore
	unsettled    []*storage.Apply
	startedLater []*storage.Apply
	err          error
	errLater     error
	holdErr      error
	reads        int
	holding      bool
	held         []string
}

func (s *branchReconcileApplyStore) GetUnsettled(context.Context) ([]*storage.Apply, error) {
	s.reads++
	if s.reads > 1 && s.errLater != nil {
		return nil, s.errLater
	}
	if s.reads > 1 {
		return append(append([]*storage.Apply{}, s.unsettled...), s.startedLater...), s.err
	}
	return s.unsettled, s.err
}

func (s *branchReconcileApplyStore) WithTargetHeld(ctx context.Context, database, dbType, environment string, fn func(context.Context) error) error {
	if s.holdErr != nil {
		return s.holdErr
	}
	s.held = append(s.held, database+"/"+dbType+"/"+environment)
	s.holding = true
	defer func() { s.holding = false }()
	return fn(ctx)
}

// branchReconcileOperationStore serves each apply's operations and their
// stored resume metadata; an operation without one has not recorded a branch.
type branchReconcileOperationStore struct {
	storage.ApplyOperationStore
	ops     map[int64][]*storage.ApplyOperation
	resumes map[int64]*storage.EngineResumeState
	// recent is what ListRecentTargets serves; lookbacks records each window
	// it was asked for.
	recent    []storage.AppliedTarget
	lookbacks []time.Duration
}

func (s *branchReconcileOperationStore) ListRecentTargets(_ context.Context, databaseType string, lookback time.Duration) ([]storage.AppliedTarget, error) {
	if databaseType != storage.DatabaseTypeVitess {
		return nil, fmt.Errorf("unexpected database type %q", databaseType)
	}
	s.lookbacks = append(s.lookbacks, lookback)
	return s.recent, nil
}

func (s *branchReconcileOperationStore) ListByApply(_ context.Context, applyID int64) ([]*storage.ApplyOperation, error) {
	return s.ops[applyID], nil
}

func (s *branchReconcileOperationStore) GetEngineResumeState(_ context.Context, operationID int64) (*storage.EngineResumeState, error) {
	resume, ok := s.resumes[operationID]
	if !ok {
		return nil, storage.ErrEngineResumeStateNotFound
	}
	return resume, nil
}

// branchReconcilePlanStore serves the plans the unsettled applies name.
type branchReconcilePlanStore struct {
	storage.PlanStore
	plans map[int64]*storage.Plan
}

func (s *branchReconcilePlanStore) GetByID(_ context.Context, id int64) (*storage.Plan, error) {
	return s.plans[id], nil
}

type branchReconcileStorage struct {
	storage.Storage
	applies    *branchReconcileApplyStore
	operations *branchReconcileOperationStore
	plans      *branchReconcilePlanStore
}

func (s *branchReconcileStorage) Applies() storage.ApplyStore                  { return s.applies }
func (s *branchReconcileStorage) ApplyOperations() storage.ApplyOperationStore { return s.operations }
func (s *branchReconcileStorage) Plans() storage.PlanStore {
	if s.plans == nil {
		return &branchReconcilePlanStore{}
	}
	return s.plans
}

func resumeMetadataNaming(t *testing.T, branch string) string {
	t.Helper()
	rs, err := planetscale.BuildResumeState(planetscale.ResumeData{BranchName: branch})
	require.NoError(t, err)
	return rs.Metadata
}

func newBranchReconcileClient(store *branchReconcileStorage, client *branchReconcileClient, metadata map[string]string) *LocalClient {
	config := LocalConfig{Database: "orders", Type: storage.DatabaseTypeVitess, Metadata: map[string]string{"organization": "block"}}
	maps.Copy(config.Metadata, metadata)
	return &LocalClient{
		config:       config,
		storage:      store,
		psClientFunc: func(_, _ string) (psclient.PSClient, error) { return client, nil },
		logger:       slog.New(slog.DiscardHandler),
	}
}

var branchReconcileNow = time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)

const branchReconcileGrace = 24 * time.Hour

func deletingBranchReconcileOptions() BranchReconcileOptions {
	return BranchReconcileOptions{Grace: branchReconcileGrace, Delete: true, Now: branchReconcileNow, Environment: "staging"}
}

// A PlanetScale database carries every kind of branch the reconcile meets. Only
// the old SchemaBot branch that no unsettled apply uses and no open deploy
// request uses is orphaned. Kept are: a branch an apply's stored resume state
// names, one an operator supplied for a PlanetScale apply, one an unsettled
// apply's plan names before the apply has recorded a branch, one a stopped
// apply's resume state names, one younger than the grace period, one exactly
// as old as it, one of unknown age, and one an open deploy request uses. Never
// candidates are: main, production branches and the configured main branch
// whatever their names, an operator's branch, and a branch whose suffix is
// not the tail of a plan identifier. A branch option on a MySQL apply names
// no PlanetScale branch. A dry run deletes nothing.
func TestReconcileOrphanedBranchesDeletesOnlyTheOrphans(t *testing.T) {
	now := branchReconcileNow
	old := now.Add(-48 * time.Hour)
	branches := []*ps.DatabaseBranch{
		{Name: "main", Production: true, CreatedAt: old},
		{Name: "schemabot-orders-aaaa0007", Production: true, CreatedAt: old},
		{Name: "schemabot-orders-aaaa0008", CreatedAt: old},
		{Name: "my-feature", CreatedAt: old},
		{Name: "schemabot-orders-feature-branch", CreatedAt: old},
		{Name: "schemabot-orders-hotfix", CreatedAt: old},
		{Name: "schemabot-orders-aaaa0001", CreatedAt: old},
		{Name: "schemabot-orders-aaaa0002", CreatedAt: old},
		{Name: "schemabot-orders-aaaa0003", CreatedAt: old},
		{Name: "schemabot-orders-aaaa0004", CreatedAt: now.Add(-time.Hour)},
		{Name: "schemabot-orders-aaaa0005"},
		{Name: "schemabot-orders-aaaa0006", CreatedAt: old},
		{Name: "schemabot-orders-aaaa0009", CreatedAt: now.Add(-branchReconcileGrace)},
		{Name: "schemabot-orders-89abcdef", CreatedAt: old},
		{Name: "schemabot-orders-aaaa000a", CreatedAt: old},
	}
	newFixture := func(t *testing.T) (*LocalClient, *branchReconcileClient, *branchReconcileApplyStore) {
		client := &branchReconcileClient{
			branches: branches,
			openDRs:  map[string][]*ps.DeployRequest{"schemabot-orders-aaaa0006": {{Number: 42, State: "open"}}},
		}
		active := &storage.Apply{ID: 1, ApplyIdentifier: "apply-active", PlanID: 1, DatabaseType: storage.DatabaseTypeVitess, State: state.Apply.ValidatingDeployRequest}
		operatorBranch := &storage.Apply{ID: 2, ApplyIdentifier: "apply-operator", PlanID: 2, DatabaseType: storage.DatabaseTypeVitess, State: state.Apply.PreparingBranch,
			Options: storage.MarshalApplyOptions(storage.ApplyOptions{Branch: "schemabot-orders-aaaa0003"})}
		queued := &storage.Apply{ID: 3, ApplyIdentifier: "apply-queued", PlanID: 3, DatabaseType: storage.DatabaseTypeVitess, State: state.Apply.Pending}
		stopped := &storage.Apply{ID: 4, ApplyIdentifier: "apply-stopped", PlanID: 4, DatabaseType: storage.DatabaseTypeVitess, State: state.Apply.Stopped}
		mysqlOption := &storage.Apply{ID: 5, ApplyIdentifier: "apply-mysql", PlanID: 5, DatabaseType: storage.DatabaseTypeMySQL, State: state.Apply.Running,
			Options: storage.MarshalApplyOptions(storage.ApplyOptions{Branch: "schemabot-orders-aaaa0001"})}
		applies := &branchReconcileApplyStore{unsettled: []*storage.Apply{active, operatorBranch, queued, stopped, mysqlOption}}
		lc := newBranchReconcileClient(&branchReconcileStorage{
			applies: applies,
			operations: &branchReconcileOperationStore{
				ops: map[int64][]*storage.ApplyOperation{1: {{ID: 11, ApplyID: 1}}, 3: {{ID: 31, ApplyID: 3}}, 4: {{ID: 41, ApplyID: 4}}},
				resumes: map[int64]*storage.EngineResumeState{
					11: {ApplyOperationID: 11, Metadata: resumeMetadataNaming(t, "schemabot-orders-aaaa0002")},
					41: {ApplyOperationID: 41, Metadata: resumeMetadataNaming(t, "schemabot-orders-aaaa000a")},
				},
			},
			plans: &branchReconcilePlanStore{plans: map[int64]*storage.Plan{
				1: {ID: 1, PlanIdentifier: "plan-0000000000bbbb0001"},
				2: {ID: 2, PlanIdentifier: "plan-0000000000bbbb0002"},
				3: {ID: 3, PlanIdentifier: "plan-0123456789abcdef"},
				4: {ID: 4, PlanIdentifier: "plan-0000000000bbbb0004"},
				5: {ID: 5, PlanIdentifier: "plan-0000000000bbbb0005"},
			}},
		}, client, map[string]string{"main_branch": "schemabot-orders-aaaa0008"})
		return lc, client, applies
	}

	t.Run("a deleting pass deletes only the orphan, under the held apply target", func(t *testing.T) {
		lc, client, applies := newFixture(t)
		client.onDelete = func() { assert.True(t, applies.holding, "the delete runs while the apply target is held") }

		result, err := lc.ReconcileOrphanedBranches(t.Context(), deletingBranchReconcileOptions())

		require.NoError(t, err)
		assert.Equal(t, []string{"schemabot-orders-aaaa0001"}, client.deleted)
		assert.Equal(t, BranchReconcileResult{Database: "orders", Candidates: 9, Kept: 8, Orphaned: 1, Deleted: 1}, result)
		assert.Equal(t, []string{"orders/vitess/staging"}, applies.held, "only the orphan's delete holds the target")
		require.NotEmpty(t, client.drRequests)
		assert.Equal(t, "open", client.drRequests[0].State, "only an open deploy request keeps a branch")
	})

	t.Run("a dry run deletes nothing and holds nothing", func(t *testing.T) {
		lc, client, applies := newFixture(t)

		result, err := lc.ReconcileOrphanedBranches(t.Context(), BranchReconcileOptions{Grace: branchReconcileGrace, Now: now})

		require.NoError(t, err)
		assert.Empty(t, client.deleted)
		assert.Empty(t, applies.held)
		assert.Equal(t, 1, result.Orphaned)
		assert.Zero(t, result.Deleted)
	})

	t.Run("a deleting pass needs the environment that names the apply target", func(t *testing.T) {
		lc, client, _ := newFixture(t)

		_, err := lc.ReconcileOrphanedBranches(t.Context(), BranchReconcileOptions{Grace: branchReconcileGrace, Delete: true, Now: now})

		require.ErrorContains(t, err, "environment is required")
		assert.Empty(t, client.deleted)
	})
}

// A branch an operator created by hand is never reclaimed, even when its name
// starts with the generated prefix and is short.
func TestReconcileKeepsAnOperatorBranchWithAShortPrefixedName(t *testing.T) {
	old := branchReconcileNow.Add(-720 * time.Hour)
	client := &branchReconcileClient{branches: []*ps.DatabaseBranch{
		{Name: "schemabot-orders-hotfix", CreatedAt: old},
		{Name: "schemabot-orders-v2", CreatedAt: old},
		{Name: "schemabot-orders-test", CreatedAt: old},
		{Name: "schemabot-orders-testing1", CreatedAt: old},
	}}
	lc := newBranchReconcileClient(&branchReconcileStorage{applies: &branchReconcileApplyStore{}, operations: &branchReconcileOperationStore{}}, client, nil)

	result, err := lc.ReconcileOrphanedBranches(t.Context(), deletingBranchReconcileOptions())

	require.NoError(t, err)
	assert.Empty(t, client.deleted, "branches no apply ever generated must be kept")
	assert.Equal(t, BranchReconcileResult{Database: "orders"}, result, "none of them is even a candidate")
}

// Anything the reconcile cannot read fails toward keeping branches: a storage
// error ends the pass before anything is deleted, a target that cannot be held
// or re-read keeps that branch, and a deploy request lookup that fails keeps
// that branch and is reported.
func TestReconcileOrphanedBranchesKeepsBranchesItCannotJudge(t *testing.T) {
	orphan := &ps.DatabaseBranch{Name: "schemabot-orders-aaaa0001", CreatedAt: branchReconcileNow.Add(-48 * time.Hour)}
	opts := deletingBranchReconcileOptions()

	t.Run("unsettled applies cannot be read", func(t *testing.T) {
		client := &branchReconcileClient{branches: []*ps.DatabaseBranch{orphan}}
		lc := newBranchReconcileClient(&branchReconcileStorage{applies: &branchReconcileApplyStore{err: errors.New("storage unavailable")}, operations: &branchReconcileOperationStore{}}, client, nil)

		_, err := lc.ReconcileOrphanedBranches(t.Context(), opts)

		require.ErrorContains(t, err, "storage unavailable")
		assert.Empty(t, client.deleted)
	})

	t.Run("the apply target cannot be held", func(t *testing.T) {
		client := &branchReconcileClient{branches: []*ps.DatabaseBranch{orphan}}
		lc := newBranchReconcileClient(&branchReconcileStorage{applies: &branchReconcileApplyStore{holdErr: context.DeadlineExceeded}, operations: &branchReconcileOperationStore{}}, client, nil)

		result, err := lc.ReconcileOrphanedBranches(t.Context(), opts)

		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.ErrorContains(t, err, "hold apply target orders/vitess/staging")
		assert.Empty(t, client.deleted)
		assert.Equal(t, BranchReconcileResult{Database: "orders", Candidates: 1, Kept: 1}, result)
	})

	t.Run("unsettled applies cannot be re-read under the held target", func(t *testing.T) {
		client := &branchReconcileClient{branches: []*ps.DatabaseBranch{orphan}}
		lc := newBranchReconcileClient(&branchReconcileStorage{applies: &branchReconcileApplyStore{errLater: errors.New("storage failover")}, operations: &branchReconcileOperationStore{}}, client, nil)

		result, err := lc.ReconcileOrphanedBranches(t.Context(), opts)

		require.ErrorContains(t, err, "storage failover")
		assert.Empty(t, client.deleted)
		assert.Equal(t, BranchReconcileResult{Database: "orders", Candidates: 1, Kept: 1}, result)
	})

	t.Run("a stored resume state cannot be decoded", func(t *testing.T) {
		client := &branchReconcileClient{branches: []*ps.DatabaseBranch{orphan}}
		lc := newBranchReconcileClient(&branchReconcileStorage{
			applies: &branchReconcileApplyStore{unsettled: []*storage.Apply{{ID: 1, ApplyIdentifier: "apply-bad", PlanID: 1, DatabaseType: storage.DatabaseTypeVitess, State: state.Apply.Running}}},
			operations: &branchReconcileOperationStore{
				ops:     map[int64][]*storage.ApplyOperation{1: {{ID: 11, ApplyID: 1}}},
				resumes: map[int64]*storage.EngineResumeState{11: {Metadata: "{not json"}},
			},
			plans: &branchReconcilePlanStore{plans: map[int64]*storage.Plan{1: {ID: 1, PlanIdentifier: "plan-0000000000bbbb0001"}}},
		}, client, nil)

		_, err := lc.ReconcileOrphanedBranches(t.Context(), opts)

		require.ErrorContains(t, err, "apply-bad")
		assert.Empty(t, client.deleted)
	})

	t.Run("an unsettled apply's plan does not exist", func(t *testing.T) {
		client := &branchReconcileClient{branches: []*ps.DatabaseBranch{orphan}}
		lc := newBranchReconcileClient(&branchReconcileStorage{
			applies:    &branchReconcileApplyStore{unsettled: []*storage.Apply{{ID: 1, ApplyIdentifier: "apply-planless", PlanID: 7, DatabaseType: storage.DatabaseTypeVitess, State: state.Apply.Pending}}},
			operations: &branchReconcileOperationStore{},
		}, client, nil)

		_, err := lc.ReconcileOrphanedBranches(t.Context(), opts)

		require.ErrorContains(t, err, "plan 7 does not exist")
		assert.Empty(t, client.deleted)
	})

	t.Run("its deploy requests cannot be read", func(t *testing.T) {
		client := &branchReconcileClient{branches: []*ps.DatabaseBranch{orphan}, drErr: map[string]error{orphan.Name: errors.New("rate limited")}}
		lc := newBranchReconcileClient(&branchReconcileStorage{applies: &branchReconcileApplyStore{}, operations: &branchReconcileOperationStore{}}, client, nil)

		result, err := lc.ReconcileOrphanedBranches(t.Context(), opts)

		require.ErrorContains(t, err, "rate limited")
		assert.Empty(t, client.deleted)
		assert.Equal(t, 1, result.Kept)
	})

	t.Run("the delete fails", func(t *testing.T) {
		client := &branchReconcileClient{branches: []*ps.DatabaseBranch{orphan}, deleteErr: errors.New("branch is busy")}
		lc := newBranchReconcileClient(&branchReconcileStorage{applies: &branchReconcileApplyStore{}, operations: &branchReconcileOperationStore{}}, client, nil)

		result, err := lc.ReconcileOrphanedBranches(t.Context(), opts)

		require.ErrorContains(t, err, "branch is busy")
		assert.Equal(t, BranchReconcileResult{Database: "orders", Candidates: 1, Orphaned: 1}, result, "orphaned, not deleted")
	})
}

// An apply re-run for the same plan reuses the branch of that name. When it
// starts after the pass first read the applies, the read under the held
// target sees it, by its plan before it has recorded a branch, and the branch
// is kept.
func TestReconcileOrphanedBranchesKeepsABranchClaimedDuringThePass(t *testing.T) {
	client := &branchReconcileClient{branches: []*ps.DatabaseBranch{{Name: "schemabot-orders-89abcdef", CreatedAt: branchReconcileNow.Add(-48 * time.Hour)}}}
	rerun := &storage.Apply{ID: 3, ApplyIdentifier: "apply-rerun", PlanID: 3, DatabaseType: storage.DatabaseTypeVitess, State: state.Apply.Pending}
	applies := &branchReconcileApplyStore{startedLater: []*storage.Apply{rerun}}
	lc := newBranchReconcileClient(&branchReconcileStorage{
		applies:    applies,
		operations: &branchReconcileOperationStore{},
		plans:      &branchReconcilePlanStore{plans: map[int64]*storage.Plan{3: {ID: 3, PlanIdentifier: "plan-0123456789abcdef"}}},
	}, client, nil)

	result, err := lc.ReconcileOrphanedBranches(t.Context(), deletingBranchReconcileOptions())

	require.NoError(t, err)
	assert.Empty(t, client.deleted)
	assert.Equal(t, []string{"orders/vitess/staging"}, applies.held)
	assert.Equal(t, BranchReconcileResult{Database: "orders", Candidates: 1, Kept: 1}, result)
}

// A data plane that resolves targets per request reconciles every PlanetScale
// target its applies recently ran against. Each is reached through the route
// an apply on it takes, so the reconcile lists the PlanetScale database that
// target resolves to and, while it deletes, holds the apply target named by
// the database and environment the applies were stored with. A target that no
// longer resolves is reported and does not stop the pass from reconciling the
// targets after it.
func TestTargetRouterReconcilesTheTargetsItRecentlyAppliedTo(t *testing.T) {
	old := branchReconcileNow.Add(-48 * time.Hour)
	planetScale := map[string]*branchReconcileClient{
		"orders-token":  {branches: []*ps.DatabaseBranch{{Name: "main", Production: true, CreatedAt: old}, {Name: "schemabot-orders-ps-aaaa0001", CreatedAt: old}}},
		"catalog-token": {branches: []*ps.DatabaseBranch{{Name: "schemabot-catalog-ps-aaaa0002", CreatedAt: old}}},
	}
	resolved := map[string]string{"orders-staging": "orders", "catalog-production": "catalog"}
	resolver := targetRouterResolverFunc(func(_ context.Context, req inventory.Request) (*inventory.Target, error) {
		name, ok := resolved[req.Target]
		if !ok {
			return nil, fmt.Errorf("target %q is not in the inventory", req.Target)
		}
		return &inventory.Target{Target: req.Target, DatabaseType: storage.DatabaseTypeVitess, Metadata: map[string]string{
			inventory.MetadataOrganization: "block",
			inventory.MetadataTokenName:    "token",
			inventory.MetadataTokenValue:   name + "-token",
			inventory.MetadataAPIURL:       "https://api.planetscale.test",
			"database":                     name + "-ps",
		}}, nil
	})
	applies := &branchReconcileApplyStore{}
	operations := &branchReconcileOperationStore{recent: []storage.AppliedTarget{
		{Database: "catalog", Environment: "production", Target: "catalog-production"},
		{Database: "decommissioned", Environment: "production", Target: "decommissioned-production"},
		{Database: "orders", Environment: "staging", Target: "orders-staging"},
	}}
	router, err := NewTargetRouter(TargetRouterConfig{
		Resolver: resolver,
		Storage:  &branchReconcileStorage{applies: applies, operations: operations},
		Logger:   slog.New(slog.DiscardHandler),
		LocalClientFactory: func(cfg LocalConfig, st storage.Storage, logger *slog.Logger) (Client, error) {
			return &LocalClient{config: cfg, storage: st, logger: logger, psClientFunc: func(_, token string) (psclient.PSClient, error) {
				return planetScale[token], nil
			}}, nil
		},
	})
	require.NoError(t, err)

	result, err := router.ReconcileRecentTargetBranches(t.Context(), BranchReconcileOptions{
		Grace: branchReconcileGrace, Delete: true, Now: branchReconcileNow, Environment: "ignored",
	})

	require.ErrorContains(t, err, `target "decommissioned-production" is not in the inventory`)
	assert.Equal(t, RecentTargetsReconcileResult{Targets: 3, Deleted: 2}, result)
	assert.Equal(t, []time.Duration{branchReconcileGrace + RecentTargetWindow}, operations.lookbacks,
		"the window outlasts the grace period, so a branch becomes reclaimable while its target is still listed")
	assert.Equal(t, []string{"schemabot-catalog-ps-aaaa0002"}, planetScale["catalog-token"].deleted)
	assert.Equal(t, []string{"schemabot-orders-ps-aaaa0001"}, planetScale["orders-token"].deleted)
	assert.Equal(t, []string{"catalog/vitess/production", "orders/vitess/staging"}, applies.held,
		"each delete holds the apply target its applies were stored under")
}
