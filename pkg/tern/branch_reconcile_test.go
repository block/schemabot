package tern

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	ps "github.com/planetscale/planetscale-go/planetscale"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine/planetscale"
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
	if c.deleteErr != nil {
		return c.deleteErr
	}
	c.deleted = append(c.deleted, req.Branch)
	return nil
}

// branchReconcileApplyStore serves the in-progress applies. When startedLater
// is set, every read after the first returns it too, as if those applies
// started while the pass ran; when errLater is set, every read after the
// first fails with it.
type branchReconcileApplyStore struct {
	storage.ApplyStore
	inProgress   []*storage.Apply
	startedLater []*storage.Apply
	err          error
	errLater     error
	reads        int
}

func (s *branchReconcileApplyStore) GetInProgress(context.Context) ([]*storage.Apply, error) {
	s.reads++
	if s.reads > 1 && s.errLater != nil {
		return nil, s.errLater
	}
	if s.reads > 1 {
		return append(append([]*storage.Apply{}, s.inProgress...), s.startedLater...), s.err
	}
	return s.inProgress, s.err
}

// branchReconcileOperationStore serves each apply's operations and their
// stored resume metadata.
type branchReconcileOperationStore struct {
	storage.ApplyOperationStore
	ops     map[int64][]*storage.ApplyOperation
	resumes map[int64]*storage.EngineResumeState
}

func (s *branchReconcileOperationStore) ListByApply(_ context.Context, applyID int64) ([]*storage.ApplyOperation, error) {
	return s.ops[applyID], nil
}

func (s *branchReconcileOperationStore) GetEngineResumeState(_ context.Context, operationID int64) (*storage.EngineResumeState, error) {
	return s.resumes[operationID], nil
}

type branchReconcileStorage struct {
	storage.Storage
	applies    *branchReconcileApplyStore
	operations *branchReconcileOperationStore
}

func (s *branchReconcileStorage) Applies() storage.ApplyStore                  { return s.applies }
func (s *branchReconcileStorage) ApplyOperations() storage.ApplyOperationStore { return s.operations }

func resumeMetadataNaming(t *testing.T, branch string) string {
	t.Helper()
	rs, err := planetscale.BuildResumeState(planetscale.ResumeData{BranchName: branch})
	require.NoError(t, err)
	return rs.Metadata
}

// A PlanetScale database carries every kind of branch the reconcile meets. Only
// the old SchemaBot branch that no in-progress apply names and no open deploy
// request uses is orphaned; main, an operator's branch, a branch an apply names,
// a branch younger than the grace period, a branch of unknown age, and a branch
// an open deploy request uses are all kept. A dry run deletes nothing.
func TestReconcileOrphanedBranchesDeletesOnlyTheOrphans(t *testing.T) {
	now := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	old := now.Add(-48 * time.Hour)
	branches := []*ps.DatabaseBranch{
		{Name: "main", Production: true, CreatedAt: old},
		{Name: "my-feature", CreatedAt: old},
		{Name: "schemabot-orders-feature-branch", CreatedAt: old},
		{Name: "schemabot-orders-0rphan01", CreatedAt: old},
		{Name: "schemabot-orders-active01", CreatedAt: old},
		{Name: "schemabot-orders-operator", CreatedAt: old},
		{Name: "schemabot-orders-young001", CreatedAt: now.Add(-time.Hour)},
		{Name: "schemabot-orders-noage001"},
		{Name: "schemabot-orders-withdr01", CreatedAt: old},
	}
	newFixture := func(t *testing.T) (*LocalClient, *branchReconcileClient) {
		client := &branchReconcileClient{
			branches: branches,
			openDRs:  map[string][]*ps.DeployRequest{"schemabot-orders-withdr01": {{Number: 42, State: "open"}}},
		}
		active := &storage.Apply{ID: 1, ApplyIdentifier: "apply-active", DatabaseType: storage.DatabaseTypeVitess, State: state.Apply.ValidatingDeployRequest}
		operatorBranch := &storage.Apply{ID: 2, ApplyIdentifier: "apply-operator", DatabaseType: storage.DatabaseTypeVitess, State: state.Apply.PreparingBranch,
			Options: storage.MarshalApplyOptions(storage.ApplyOptions{Branch: "schemabot-orders-operator"})}
		lc := &LocalClient{
			config: LocalConfig{Database: "orders", Type: storage.DatabaseTypeVitess, Metadata: map[string]string{"organization": "block"}},
			storage: &branchReconcileStorage{
				applies: &branchReconcileApplyStore{inProgress: []*storage.Apply{active, operatorBranch}},
				operations: &branchReconcileOperationStore{
					ops:     map[int64][]*storage.ApplyOperation{1: {{ID: 11, ApplyID: 1}}},
					resumes: map[int64]*storage.EngineResumeState{11: {ApplyOperationID: 11, Metadata: resumeMetadataNaming(t, "schemabot-orders-active01")}},
				},
			},
			psClientFunc: func(_, _ string) (psclient.PSClient, error) { return client, nil },
			logger:       slog.New(slog.DiscardHandler),
		}
		return lc, client
	}

	t.Run("a deleting pass deletes only the orphan", func(t *testing.T) {
		lc, client := newFixture(t)
		result, err := lc.ReconcileOrphanedBranches(t.Context(), BranchReconcileOptions{Grace: 24 * time.Hour, Delete: true, Now: now})

		require.NoError(t, err)
		assert.Equal(t, []string{"schemabot-orders-0rphan01"}, client.deleted)
		assert.Equal(t, BranchReconcileResult{Database: "orders", Candidates: 6, Kept: 5, Orphaned: 1, Deleted: 1}, result)
		require.NotEmpty(t, client.drRequests)
		assert.Equal(t, "open", client.drRequests[0].State, "only an open deploy request keeps a branch")
	})

	t.Run("a dry run deletes nothing", func(t *testing.T) {
		lc, client := newFixture(t)
		result, err := lc.ReconcileOrphanedBranches(t.Context(), BranchReconcileOptions{Grace: 24 * time.Hour, Now: now})

		require.NoError(t, err)
		assert.Empty(t, client.deleted)
		assert.Equal(t, 1, result.Orphaned)
		assert.Zero(t, result.Deleted)
	})
}

// Anything the reconcile cannot read fails toward keeping branches: a storage
// error ends the pass before anything is deleted, and a deploy request lookup
// that fails keeps that branch and is reported.
func TestReconcileOrphanedBranchesKeepsBranchesItCannotJudge(t *testing.T) {
	now := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	orphan := &ps.DatabaseBranch{Name: "schemabot-orders-0rphan01", CreatedAt: now.Add(-48 * time.Hour)}
	newClient := func(store *branchReconcileStorage, client *branchReconcileClient) *LocalClient {
		return &LocalClient{
			config:       LocalConfig{Database: "orders", Type: storage.DatabaseTypeVitess, Metadata: map[string]string{"organization": "block"}},
			storage:      store,
			psClientFunc: func(_, _ string) (psclient.PSClient, error) { return client, nil },
			logger:       slog.New(slog.DiscardHandler),
		}
	}
	opts := BranchReconcileOptions{Grace: 24 * time.Hour, Delete: true, Now: now}

	t.Run("in-progress applies cannot be read", func(t *testing.T) {
		client := &branchReconcileClient{branches: []*ps.DatabaseBranch{orphan}}
		lc := newClient(&branchReconcileStorage{applies: &branchReconcileApplyStore{err: errors.New("storage unavailable")}, operations: &branchReconcileOperationStore{}}, client)

		_, err := lc.ReconcileOrphanedBranches(t.Context(), opts)

		require.ErrorContains(t, err, "storage unavailable")
		assert.Empty(t, client.deleted)
	})

	t.Run("in-progress applies cannot be re-read before the delete", func(t *testing.T) {
		client := &branchReconcileClient{branches: []*ps.DatabaseBranch{orphan}}
		lc := newClient(&branchReconcileStorage{applies: &branchReconcileApplyStore{errLater: errors.New("storage failover")}, operations: &branchReconcileOperationStore{}}, client)

		result, err := lc.ReconcileOrphanedBranches(t.Context(), opts)

		require.ErrorContains(t, err, "storage failover")
		assert.Empty(t, client.deleted)
		assert.Equal(t, 1, result.Kept)
	})

	t.Run("a stored resume state cannot be decoded", func(t *testing.T) {
		client := &branchReconcileClient{branches: []*ps.DatabaseBranch{orphan}}
		lc := newClient(&branchReconcileStorage{
			applies: &branchReconcileApplyStore{inProgress: []*storage.Apply{{ID: 1, ApplyIdentifier: "apply-bad", DatabaseType: storage.DatabaseTypeVitess, State: state.Apply.Running}}},
			operations: &branchReconcileOperationStore{
				ops:     map[int64][]*storage.ApplyOperation{1: {{ID: 11, ApplyID: 1}}},
				resumes: map[int64]*storage.EngineResumeState{11: {Metadata: "{not json"}},
			},
		}, client)

		_, err := lc.ReconcileOrphanedBranches(t.Context(), opts)

		require.ErrorContains(t, err, "apply-bad")
		assert.Empty(t, client.deleted)
	})

	t.Run("its deploy requests cannot be read", func(t *testing.T) {
		client := &branchReconcileClient{branches: []*ps.DatabaseBranch{orphan}, drErr: map[string]error{orphan.Name: errors.New("rate limited")}}
		lc := newClient(&branchReconcileStorage{applies: &branchReconcileApplyStore{}, operations: &branchReconcileOperationStore{}}, client)

		result, err := lc.ReconcileOrphanedBranches(t.Context(), opts)

		require.ErrorContains(t, err, "rate limited")
		assert.Empty(t, client.deleted)
		assert.Equal(t, 1, result.Kept)
	})
}

// An apply re-run for the same plan reuses the branch of that name. When it
// starts after the pass first read the in-progress applies, the re-read before
// the delete sees it and the branch is kept.
func TestReconcileOrphanedBranchesKeepsABranchClaimedDuringThePass(t *testing.T) {
	now := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	client := &branchReconcileClient{branches: []*ps.DatabaseBranch{{Name: "schemabot-orders-89abcdef", CreatedAt: now.Add(-48 * time.Hour)}}}
	rerun := &storage.Apply{ID: 3, ApplyIdentifier: "apply-rerun", DatabaseType: storage.DatabaseTypeVitess, State: state.Apply.PreparingBranch}
	lc := &LocalClient{
		config: LocalConfig{Database: "orders", Type: storage.DatabaseTypeVitess, Metadata: map[string]string{"organization": "block"}},
		storage: &branchReconcileStorage{
			applies: &branchReconcileApplyStore{startedLater: []*storage.Apply{rerun}},
			operations: &branchReconcileOperationStore{
				ops:     map[int64][]*storage.ApplyOperation{3: {{ID: 31, ApplyID: 3}}},
				resumes: map[int64]*storage.EngineResumeState{31: {ApplyOperationID: 31, Metadata: resumeMetadataNaming(t, "schemabot-orders-89abcdef")}},
			},
		},
		psClientFunc: func(_, _ string) (psclient.PSClient, error) { return client, nil },
		logger:       slog.New(slog.DiscardHandler),
	}

	result, err := lc.ReconcileOrphanedBranches(t.Context(), BranchReconcileOptions{Grace: 24 * time.Hour, Delete: true, Now: now})

	require.NoError(t, err)
	assert.Empty(t, client.deleted)
	assert.Equal(t, BranchReconcileResult{Database: "orders", Candidates: 1, Kept: 1}, result)
}
