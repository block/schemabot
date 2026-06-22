package tern

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
)

// fakePlanStore lets a test script the plan Get/Create behavior the plan
// materialization path depends on.
type fakePlanStore struct {
	storage.PlanStore
	getFn     func(planIdentifier string) (*storage.Plan, error)
	createID  int64
	createErr error
	created   *storage.Plan
}

func (f *fakePlanStore) Get(_ context.Context, planIdentifier string) (*storage.Plan, error) {
	return f.getFn(planIdentifier)
}

func (f *fakePlanStore) Create(_ context.Context, plan *storage.Plan) (int64, error) {
	f.created = plan
	return f.createID, f.createErr
}

type fakePlanStorage struct {
	storage.Storage
	plans storage.PlanStore
}

func (s *fakePlanStorage) Plans() storage.PlanStore { return s.plans }

func newPlanMaterializeClient(plans storage.PlanStore) *LocalClient {
	return &LocalClient{
		config:  LocalConfig{Database: "testapp", Type: storage.DatabaseTypeMySQL},
		storage: &fakePlanStorage{plans: plans},
		logger:  slog.Default(),
	}
}

// A deployment that planned locally resolves its own stored plan and never
// materializes a new one from the dispatch request.
func TestPlanForApplyRequest_LocalPlanWins(t *testing.T) {
	existing := &storage.Plan{ID: 11, PlanIdentifier: "plan_local"}
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return existing, nil }}
	c := newPlanMaterializeClient(store)

	got, err := c.planForApplyRequest(t.Context(), &ternv1.ApplyRequest{
		PlanId:     "plan_local",
		DdlChanges: []*ternv1.TableChange{{TableName: "users", Ddl: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER}},
	})

	require.NoError(t, err)
	assert.Same(t, existing, got)
	assert.Nil(t, store.created, "must not materialize when a local plan exists")
}

// A non-primary deployment with no local plan materializes one from the
// authoritative DDL changes and schema files carried by the dispatch request.
func TestPlanForApplyRequest_MaterializesFromRequest(t *testing.T) {
	store := &fakePlanStore{
		getFn:    func(string) (*storage.Plan, error) { return nil, nil },
		createID: 42,
	}
	c := newPlanMaterializeClient(store)

	got, err := c.planForApplyRequest(t.Context(), &ternv1.ApplyRequest{
		PlanId:      "plan_remote",
		Environment: "staging",
		Target:      "testapp-us",
		DdlChanges: []*ternv1.TableChange{
			{TableName: "users", Ddl: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Namespace: "testapp"},
		},
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"testapp": {Files: map[string]string{"users.sql": "CREATE TABLE `users` ..."}},
		},
	})

	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, int64(42), got.ID)
	assert.Equal(t, "plan_remote", got.PlanIdentifier)
	assert.Equal(t, "testapp", got.Database)
	assert.Equal(t, "testapp-us", got.Target)
	assert.Equal(t, "staging", got.Environment)

	require.NotNil(t, store.created)
	ns := store.created.Namespaces["testapp"]
	require.NotNil(t, ns)
	require.Len(t, ns.Tables, 1)
	assert.Equal(t, "users", ns.Tables[0].Table)
	assert.Equal(t, "alter", ns.Tables[0].Operation)
	assert.Contains(t, store.created.SchemaFiles, "testapp")
}

// With no local plan and a request that carries no DDL or schema files there is
// nothing to materialize, so the apply resolves to no plan (the caller then
// returns the "plan not found" rejection).
func TestPlanForApplyRequest_NoPayloadResolvesNil(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	c := newPlanMaterializeClient(store)

	got, err := c.planForApplyRequest(t.Context(), &ternv1.ApplyRequest{PlanId: "plan_missing"})

	require.NoError(t, err)
	assert.Nil(t, got)
	assert.Nil(t, store.created)
}

// If two drivers race to materialize the same plan, the loser's Create fails on
// the duplicate identifier and it reuses the winner's row instead of erroring.
func TestPlanForApplyRequest_DuplicateCreateReloads(t *testing.T) {
	winner := &storage.Plan{ID: 7, PlanIdentifier: "plan_race"}
	calls := 0
	store := &fakePlanStore{
		getFn: func(string) (*storage.Plan, error) {
			calls++
			if calls == 1 {
				return nil, nil // first lookup: not yet materialized
			}
			return winner, nil // reload after the duplicate Create
		},
		createErr: errors.New("duplicate plan_identifier"),
	}
	c := newPlanMaterializeClient(store)

	got, err := c.planForApplyRequest(t.Context(), &ternv1.ApplyRequest{
		PlanId:     "plan_race",
		DdlChanges: []*ternv1.TableChange{{TableName: "users", Ddl: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Namespace: "testapp"}},
	})

	require.NoError(t, err)
	assert.Same(t, winner, got)
}

// namespacesFromApplyRequest groups DDL by namespace, falls back to the client
// database for unnamespaced changes, drops vschema table changes (re-derived at
// apply time), and recovers the vschema artifact from the schema files.
func TestNamespacesFromApplyRequest(t *testing.T) {
	changes := []*ternv1.TableChange{
		{TableName: "users", Ddl: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Namespace: "shop"},
		{TableName: "orders", Ddl: "CREATE TABLE `orders` (`id` bigint)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_CREATE, Namespace: ""},
		{TableName: "VSchema: shop", ChangeType: ternv1.ChangeType_CHANGE_TYPE_VSCHEMA, Namespace: "shop"},
	}
	schemaFiles := schema.SchemaFiles{
		"shop": {Files: map[string]string{vSchemaArtifactName: `{"sharded":true}`}},
	}

	got := namespacesFromApplyRequest(changes, schemaFiles, "fallbackdb")

	require.Contains(t, got, "shop")
	require.Contains(t, got, "fallbackdb")

	shop := got["shop"]
	require.Len(t, shop.Tables, 1, "vschema change must not become a table change")
	assert.Equal(t, "users", shop.Tables[0].Table)
	assert.Equal(t, "alter", shop.Tables[0].Operation)
	assert.Equal(t, `{"sharded":true}`, shop.Artifacts[vSchemaArtifactName])

	fallback := got["fallbackdb"]
	require.Len(t, fallback.Tables, 1)
	assert.Equal(t, "orders", fallback.Tables[0].Table)
	assert.Equal(t, "create", fallback.Tables[0].Operation)
}
