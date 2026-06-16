package tern

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/inventory"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

type targetRouterStorage struct {
	storage.Storage
	applies storage.ApplyStore
	plans   storage.PlanStore
}

func (s targetRouterStorage) Applies() storage.ApplyStore { return s.applies }

func (s targetRouterStorage) Plans() storage.PlanStore { return s.plans }

func (s targetRouterStorage) Ping(context.Context) error { return nil }

type targetRouterPlanStore struct {
	storage.PlanStore
	byID         map[int64]*storage.Plan
	byIdentifier map[string]*storage.Plan
}

func (s targetRouterPlanStore) GetByID(_ context.Context, id int64) (*storage.Plan, error) {
	plan := s.byID[id]
	if plan == nil {
		return nil, nil
	}
	copy := *plan
	return &copy, nil
}

func (s targetRouterPlanStore) Get(_ context.Context, identifier string) (*storage.Plan, error) {
	plan := s.byIdentifier[identifier]
	if plan == nil {
		return nil, nil
	}
	copy := *plan
	return &copy, nil
}

type targetRouterApplyStore struct {
	storage.ApplyStore
	byID         map[int64]*storage.Apply
	byIdentifier map[string]*storage.Apply
}

func (s targetRouterApplyStore) Get(_ context.Context, id int64) (*storage.Apply, error) {
	apply := s.byID[id]
	if apply == nil {
		return nil, nil
	}
	copy := *apply
	return &copy, nil
}

func (s targetRouterApplyStore) GetByApplyIdentifier(_ context.Context, applyIdentifier string) (*storage.Apply, error) {
	apply := s.byIdentifier[applyIdentifier]
	if apply == nil {
		return nil, nil
	}
	copy := *apply
	return &copy, nil
}

type targetRouterRecordingClient struct {
	pullReq            *ternv1.PullSchemaRequest
	planReq            *ternv1.PlanRequest
	applyReq           *ternv1.ApplyRequest
	progressReq        *ternv1.ProgressRequest
	resumeApply        *storage.Apply
	targetDSN          string
	targetMetadata     map[string]string
	pendingObserverSet bool
	observerApplyID    int64
	closed             bool
}

func (c *targetRouterRecordingClient) PullSchema(_ context.Context, req *ternv1.PullSchemaRequest) (*ternv1.PullSchemaResponse, error) {
	c.pullReq = req
	return &ternv1.PullSchemaResponse{Database: req.Database, Type: req.Type}, nil
}

func (c *targetRouterRecordingClient) Plan(_ context.Context, req *ternv1.PlanRequest) (*ternv1.PlanResponse, error) {
	c.planReq = req
	return &ternv1.PlanResponse{PlanId: "plan-routed"}, nil
}

func (c *targetRouterRecordingClient) Apply(_ context.Context, req *ternv1.ApplyRequest) (*ternv1.ApplyResponse, error) {
	c.applyReq = req
	return &ternv1.ApplyResponse{Accepted: true, ApplyId: "apply-routed"}, nil
}

func (c *targetRouterRecordingClient) Progress(_ context.Context, req *ternv1.ProgressRequest) (*ternv1.ProgressResponse, error) {
	c.progressReq = req
	return &ternv1.ProgressResponse{ApplyId: req.ApplyId}, nil
}

func (c *targetRouterRecordingClient) Cutover(context.Context, *ternv1.CutoverRequest) (*ternv1.CutoverResponse, error) {
	return &ternv1.CutoverResponse{Accepted: true}, nil
}

func (c *targetRouterRecordingClient) Stop(context.Context, *ternv1.StopRequest) (*ternv1.StopResponse, error) {
	return &ternv1.StopResponse{Accepted: true}, nil
}

func (c *targetRouterRecordingClient) Start(context.Context, *ternv1.StartRequest) (*ternv1.StartResponse, error) {
	return &ternv1.StartResponse{Accepted: true}, nil
}

func (c *targetRouterRecordingClient) Volume(context.Context, *ternv1.VolumeRequest) (*ternv1.VolumeResponse, error) {
	return &ternv1.VolumeResponse{Accepted: true}, nil
}

func (c *targetRouterRecordingClient) Revert(context.Context, *ternv1.RevertRequest) (*ternv1.RevertResponse, error) {
	return &ternv1.RevertResponse{Accepted: true}, nil
}

func (c *targetRouterRecordingClient) SkipRevert(context.Context, *ternv1.SkipRevertRequest) (*ternv1.SkipRevertResponse, error) {
	return &ternv1.SkipRevertResponse{Accepted: true}, nil
}

func (c *targetRouterRecordingClient) Health(context.Context) error { return nil }

func (c *targetRouterRecordingClient) ResumeApply(_ context.Context, apply *storage.Apply) error {
	c.resumeApply = apply
	return nil
}

func (c *targetRouterRecordingClient) ResumeApplyOperation(_ context.Context, apply *storage.Apply, _ int64) error {
	c.resumeApply = apply
	return nil
}

func (c *targetRouterRecordingClient) Endpoint() string { return "recording" }

func (c *targetRouterRecordingClient) IsRemote() bool { return false }

func (c *targetRouterRecordingClient) SetPendingObserver(observer ProgressObserver) {
	c.pendingObserverSet = observer != nil
}

func (c *targetRouterRecordingClient) SetObserver(applyID int64, _ ProgressObserver) {
	c.observerApplyID = applyID
}

func (c *targetRouterRecordingClient) Close() error {
	c.closed = true
	return nil
}

type targetRouterNoopObserver struct{}

func (targetRouterNoopObserver) OnProgress(*storage.Apply, []*storage.Task) {}

func (targetRouterNoopObserver) OnTerminal(*storage.Apply, []*storage.Task) {}

type targetRouterResolverFunc func(context.Context, inventory.Request) (*inventory.Target, error)

func (f targetRouterResolverFunc) ResolveTarget(ctx context.Context, req inventory.Request) (*inventory.Target, error) {
	return f(ctx, req)
}

func TestTargetRouterRoutesPlanThroughStaticTarget(t *testing.T) {
	resolver := newStaticResolver(t)
	created := make(map[string]*targetRouterRecordingClient)
	router := newTargetRouterForTest(t, resolver, nil, nil, created)

	resp, err := router.Plan(t.Context(), &ternv1.PlanRequest{
		Database:    "orders-logical",
		Type:        storage.DatabaseTypeMySQL,
		Environment: "production",
		Target:      "dsid-orders-prod",
	})

	require.NoError(t, err)
	assert.Equal(t, "plan-routed", resp.PlanId)
	client := created["orders-logical"]
	require.NotNil(t, client)
	require.NotNil(t, client.planReq)
	assert.Equal(t, "orders-logical", client.planReq.Database)
	assert.Equal(t, storage.DatabaseTypeMySQL, client.planReq.Type)
	assert.Equal(t, "dsid-orders-prod", client.planReq.Target)
	assert.Equal(t, "root@tcp(localhost:3306)/", client.targetDSN)

	_, err = router.PullSchema(t.Context(), &ternv1.PullSchemaRequest{Database: "orders-logical", Type: storage.DatabaseTypeMySQL, Environment: "production", Target: "dsid-orders-prod"})
	require.NoError(t, err)
	assert.Len(t, created, 1, "the router should cache LocalClients by resolved target route and namespace")
}

func TestTargetRouterApplyUsesTargetScopedPendingObserver(t *testing.T) {
	resolver := newStaticResolver(t)
	created := make(map[string]*targetRouterRecordingClient)
	router := newTargetRouterForTest(t, resolver, nil, nil, created)
	require.NoError(t, router.SetPendingObserverForTarget("dsid-orders-prod", targetRouterNoopObserver{}))

	resp, err := router.Apply(t.Context(), &ternv1.ApplyRequest{
		PlanId:      "plan-1",
		Database:    "orders-logical",
		Type:        storage.DatabaseTypeMySQL,
		Environment: "production",
		Target:      "dsid-orders-prod",
	})

	require.NoError(t, err)
	assert.True(t, resp.Accepted)
	client := created["orders-logical"]
	require.NotNil(t, client)
	assert.True(t, client.pendingObserverSet)
	require.NotNil(t, client.applyReq)
	assert.Equal(t, "orders-logical", client.applyReq.Database)
	assert.Equal(t, "dsid-orders-prod", client.applyReq.Target)
	assert.Equal(t, "dsid-orders-prod", client.applyReq.Options["target"])
}

func TestTargetRouterCachesByTargetTypeAndEnvironment(t *testing.T) {
	resolver := targetRouterResolverFunc(func(_ context.Context, req inventory.Request) (*inventory.Target, error) {
		return &inventory.Target{
			Target:       req.Target,
			DatabaseType: req.DatabaseType,
			DSN:          "root@tcp(localhost:3306)/",
		}, nil
	})
	created := make(map[string]*targetRouterRecordingClient)
	router := newTargetRouterForTest(t, resolver, nil, nil, created)
	require.NoError(t, router.SetPendingObserverForRequest(inventory.Request{
		Target:       "dsid-orders",
		DatabaseType: storage.DatabaseTypeMySQL,
		Environment:  "production",
	}, targetRouterNoopObserver{}))

	_, err := router.Apply(t.Context(), &ternv1.ApplyRequest{Database: "orders", Target: "dsid-orders", Type: storage.DatabaseTypeMySQL, Environment: "staging"})
	require.NoError(t, err)
	_, err = router.Apply(t.Context(), &ternv1.ApplyRequest{Database: "orders", Target: "dsid-orders", Type: storage.DatabaseTypeMySQL, Environment: "production"})
	require.NoError(t, err)

	assert.Len(t, created, 2, "the same target key can resolve differently by environment")
	var pendingObserverClients int
	for _, client := range created {
		if client.pendingObserverSet {
			pendingObserverClients++
		}
	}
	assert.Equal(t, 1, pendingObserverClients)
}

func TestTargetRouterRoutesStoredApplyByTargetOption(t *testing.T) {
	resolver := newStaticResolver(t)
	created := make(map[string]*targetRouterRecordingClient)
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-42",
		Database:        "orders",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "production",
	}
	apply.SetOptions(storage.ApplyOptions{Target: "dsid-orders-prod"})
	store := targetRouterApplyStore{
		byID:         map[int64]*storage.Apply{42: apply},
		byIdentifier: map[string]*storage.Apply{"apply-42": apply},
	}
	router := newTargetRouterForTest(t, resolver, store, nil, created)
	router.SetObserver(42, targetRouterNoopObserver{})

	err := router.ResumeApply(t.Context(), apply)

	require.NoError(t, err)
	client := created["orders"]
	require.NotNil(t, client)
	assert.Equal(t, int64(42), client.observerApplyID)
	assert.Equal(t, apply.ApplyIdentifier, client.resumeApply.ApplyIdentifier)

	_, err = router.Progress(t.Context(), &ternv1.ProgressRequest{ApplyId: "apply-42", Environment: "production"})
	require.NoError(t, err)
	assert.Equal(t, "apply-42", client.progressReq.ApplyId)
}

func TestTargetRouterRoutesStoredApplyByStoredPlanTarget(t *testing.T) {
	resolver := newStaticResolver(t)
	created := make(map[string]*targetRouterRecordingClient)
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-42",
		PlanID:          7,
		Database:        "orders",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "production",
	}
	applyStore := targetRouterApplyStore{
		byID:         map[int64]*storage.Apply{42: apply},
		byIdentifier: map[string]*storage.Apply{"apply-42": apply},
	}
	planStore := targetRouterPlanStore{byID: map[int64]*storage.Plan{
		7: {
			ID:       7,
			Target:   "dsid-orders-prod",
			Database: "orders",
		},
	}}
	router := newTargetRouterForTest(t, resolver, applyStore, planStore, created)

	err := router.ResumeApply(t.Context(), apply)

	require.NoError(t, err)
	client := created["orders"]
	require.NotNil(t, client)
	assert.Equal(t, apply.ApplyIdentifier, client.resumeApply.ApplyIdentifier)
}

// An apply executes a previously created plan, so the plan is authoritative for
// routing. A request that carries only the schema namespace (Database) and no
// opaque Target must route by the plan's stored target, never by treating the
// namespace as the target.
func TestTargetRouterApplyRoutesByPlanTargetWhenRequestOmitsTarget(t *testing.T) {
	resolver := newStaticResolver(t)
	created := make(map[string]*targetRouterRecordingClient)
	planStore := targetRouterPlanStore{byIdentifier: map[string]*storage.Plan{
		"plan-7": {
			PlanIdentifier: "plan-7",
			Target:         "dsid-orders-prod",
			Database:       "orders",
			DatabaseType:   storage.DatabaseTypeMySQL,
			Environment:    "production",
		},
	}}
	router := newTargetRouterForTest(t, resolver, nil, planStore, created)

	resp, err := router.Apply(t.Context(), &ternv1.ApplyRequest{
		PlanId:   "plan-7",
		Database: "orders",
	})

	require.NoError(t, err)
	assert.True(t, resp.Accepted)
	client := created["orders"]
	require.NotNil(t, client)
	require.NotNil(t, client.applyReq)
	assert.Equal(t, "orders", client.applyReq.Database)
	assert.Equal(t, "dsid-orders-prod", client.applyReq.Target)
	assert.Equal(t, "dsid-orders-prod", client.applyReq.Options["target"])
	assert.Equal(t, storage.DatabaseTypeMySQL, client.applyReq.Type)
	assert.Equal(t, "production", client.applyReq.Environment)
}

// The plan is authoritative: a request that supplies a Target contradicting the
// stored plan target must fail closed rather than silently override the
// plan-time route.
func TestTargetRouterApplyFailsClosedOnRequestTargetMismatch(t *testing.T) {
	resolver := newStaticResolver(t)
	created := make(map[string]*targetRouterRecordingClient)
	planStore := targetRouterPlanStore{byIdentifier: map[string]*storage.Plan{
		"plan-7": {
			PlanIdentifier: "plan-7",
			Target:         "dsid-orders-prod",
			Database:       "orders",
		},
	}}
	router := newTargetRouterForTest(t, resolver, nil, planStore, created)

	_, err := router.Apply(t.Context(), &ternv1.ApplyRequest{
		PlanId:   "plan-7",
		Target:   "dsid-wrong",
		Database: "orders",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not match plan")
	assert.Empty(t, created, "no client should be created when the request target contradicts the plan")
}

// A resolver that returns an incomplete target (here, an empty DSN) must fail
// closed at routing time rather than building a client that errors confusingly
// on first connect.
func TestTargetRouterFailsClosedOnIncompleteResolvedTarget(t *testing.T) {
	resolver := targetRouterResolverFunc(func(context.Context, inventory.Request) (*inventory.Target, error) {
		return &inventory.Target{Target: "dsid-orders-prod", DatabaseType: storage.DatabaseTypeMySQL, DSN: ""}, nil
	})
	created := make(map[string]*targetRouterRecordingClient)
	router := newTargetRouterForTest(t, resolver, nil, nil, created)

	_, err := router.Plan(t.Context(), &ternv1.PlanRequest{Database: "orders", Target: "dsid-orders-prod", Type: storage.DatabaseTypeMySQL})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "incomplete target")
	assert.Empty(t, created, "no client should be created for an incomplete resolved target")
}

// A Vitess target reaches the database through the PlanetScale API, so it
// carries connection details in metadata and has no DSN. Routing must accept it
// and pass the metadata through to the client rather than rejecting the empty
// DSN as incomplete.
func TestTargetRouterAcceptsMetadataOnlyVitessTarget(t *testing.T) {
	resolver := targetRouterResolverFunc(func(context.Context, inventory.Request) (*inventory.Target, error) {
		return &inventory.Target{
			Target:       "dsid-orders-vitess",
			DatabaseType: storage.DatabaseTypeVitess,
			Metadata: map[string]string{
				inventory.MetadataOrganization: "acme",
				inventory.MetadataTokenName:    "tok-id",
				inventory.MetadataTokenValue:   "tok-secret",
				inventory.MetadataAPIURL:       "https://localscale.test",
			},
		}, nil
	})
	created := make(map[string]*targetRouterRecordingClient)
	router := newTargetRouterForTest(t, resolver, nil, nil, created)

	_, err := router.Plan(t.Context(), &ternv1.PlanRequest{Database: "orders", Target: "dsid-orders-vitess", Type: storage.DatabaseTypeVitess})

	require.NoError(t, err)
	require.Len(t, created, 1, "the router should build a client for a metadata-only Vitess target")
	client := created["orders"]
	require.NotNil(t, client)
	assert.Empty(t, client.targetDSN, "a Vitess client connects via metadata, not a DSN")
	assert.Equal(t, "acme", client.targetMetadata[inventory.MetadataOrganization])
	assert.Equal(t, "tok-secret", client.targetMetadata[inventory.MetadataTokenValue])
}

// A Vitess target missing its PlanetScale metadata cannot open a connection, so
// routing must fail closed with the missing field rather than building a client
// that errors confusingly on first API call.
func TestTargetRouterFailsClosedOnVitessTargetMissingMetadata(t *testing.T) {
	resolver := targetRouterResolverFunc(func(context.Context, inventory.Request) (*inventory.Target, error) {
		return &inventory.Target{
			Target:       "dsid-orders-vitess",
			DatabaseType: storage.DatabaseTypeVitess,
			Metadata: map[string]string{
				inventory.MetadataOrganization: "acme",
				inventory.MetadataTokenName:    "tok-id",
				inventory.MetadataAPIURL:       "https://localscale.test",
			},
		}, nil
	})
	created := make(map[string]*targetRouterRecordingClient)
	router := newTargetRouterForTest(t, resolver, nil, nil, created)

	_, err := router.Plan(t.Context(), &ternv1.PlanRequest{Database: "orders", Target: "dsid-orders-vitess", Type: storage.DatabaseTypeVitess})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "incomplete target")
	assert.Contains(t, err.Error(), inventory.MetadataTokenValue)
	assert.Empty(t, created, "no client should be created for an incomplete Vitess target")
}

func TestTargetRouterCloseClosesCachedClients(t *testing.T) {
	resolver := newStaticResolver(t)
	created := make(map[string]*targetRouterRecordingClient)
	router := newTargetRouterForTest(t, resolver, nil, nil, created)
	_, err := router.Plan(t.Context(), &ternv1.PlanRequest{Database: "orders", Target: "dsid-orders-prod", Type: storage.DatabaseTypeMySQL})
	require.NoError(t, err)

	require.NoError(t, router.Close())
	assert.True(t, created["orders"].closed)
}

func newStaticResolver(t *testing.T) *inventory.StaticResolver {
	t.Helper()
	resolver, err := inventory.NewStaticResolver(inventory.StaticConfig{Targets: map[string]inventory.StaticTarget{
		"dsid-orders-prod": {
			DatabaseType: storage.DatabaseTypeMySQL,
			DSN:          "root@tcp(localhost:3306)/",
			Metadata:     map[string]string{"pending_drops": "false"},
		},
	}})
	require.NoError(t, err)
	return resolver
}

func newTargetRouterForTest(t *testing.T, resolver inventory.Resolver, applyStore storage.ApplyStore, planStore storage.PlanStore, created map[string]*targetRouterRecordingClient) *TargetRouter {
	t.Helper()
	if applyStore == nil {
		applyStore = targetRouterApplyStore{}
	}
	router, err := NewTargetRouter(TargetRouterConfig{
		Resolver: resolver,
		Storage:  targetRouterStorage{applies: applyStore, plans: planStore},
		Logger:   slog.Default(),
		LocalClientFactory: func(cfg LocalConfig, _ storage.Storage, _ *slog.Logger) (Client, error) {
			client := &targetRouterRecordingClient{targetDSN: cfg.TargetDSN, targetMetadata: cfg.Metadata}
			key := cfg.Database
			if existing := created[key]; existing != nil {
				key = fmt.Sprintf("%s#%d", cfg.Database, len(created)+1)
			}
			created[key] = client
			return client, nil
		},
	})
	require.NoError(t, err)
	return router
}
