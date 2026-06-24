package tern

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/statement"
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

// fakePlanEngine implements only engine.Plan so the drift guard can recompute a
// local plan in tests without a live database. All other engine methods are
// inherited from the embedded nil interface and must not be called.
type fakePlanEngine struct {
	engine.Engine
	planFn func(context.Context, *engine.PlanRequest) (*engine.PlanResult, error)
}

func (e fakePlanEngine) Plan(ctx context.Context, req *engine.PlanRequest) (*engine.PlanResult, error) {
	return e.planFn(ctx, req)
}

// newPlanMaterializeClientWithPlan returns a materialize client whose drift
// guard recomputes the given plan result against "live" schema. The DB-bearing
// TargetDSN keeps planWithEngine on the single-namespace path.
func newPlanMaterializeClientWithPlan(plans storage.PlanStore, result *engine.PlanResult) *LocalClient {
	c := newPlanMaterializeClient(plans)
	c.config.TargetDSN = "user:pass@tcp(127.0.0.1:3306)/testapp"
	c.spiritEngine = fakePlanEngine{
		planFn: func(context.Context, *engine.PlanRequest) (*engine.PlanResult, error) { return result, nil },
	}
	return c
}

// alterUsersEmailPlan is the recomputed plan that exactly matches the reviewed
// ALTER used across the materialize-path tests.
func alterUsersEmailPlan() *engine.PlanResult {
	return &engine.PlanResult{
		Changes: []engine.SchemaChange{{
			Namespace: "testapp",
			TableChanges: []engine.TableChange{{
				Table:     "users",
				Operation: statement.StatementAlterTable,
				DDL:       "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
			}},
		}},
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
	c := newPlanMaterializeClientWithPlan(store, alterUsersEmailPlan())

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
	c := newPlanMaterializeClientWithPlan(store, alterUsersEmailPlan())

	got, err := c.planForApplyRequest(t.Context(), &ternv1.ApplyRequest{
		PlanId:     "plan_race",
		DdlChanges: []*ternv1.TableChange{{TableName: "users", Ddl: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Namespace: "testapp"}},
	})

	require.NoError(t, err)
	assert.Same(t, winner, got)
}

// namespacesFromApplyRequest groups DDL by namespace, falls back to the client
// database for unnamespaced changes, drops vschema table changes (re-derived at
// apply time), and recovers the vschema artifact from the schema files only for
// namespaces with an explicit vschema change.
func TestNamespacesFromApplyRequest(t *testing.T) {
	c := newPlanMaterializeClient(&fakePlanStore{})
	changes := []*ternv1.TableChange{
		{TableName: "users", Ddl: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Namespace: "shop"},
		{TableName: "orders", Ddl: "CREATE TABLE `orders` (`id` bigint)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_CREATE, Namespace: ""},
		{TableName: "VSchema: shop", ChangeType: ternv1.ChangeType_CHANGE_TYPE_VSCHEMA, Namespace: "shop"},
	}
	schemaFiles := schema.SchemaFiles{
		"shop": {Files: map[string]string{vSchemaArtifactName: `{"sharded":true}`}},
	}

	got, err := c.namespacesFromApplyRequest(changes, schemaFiles)
	require.NoError(t, err)

	require.Contains(t, got, "shop")
	require.Contains(t, got, "testapp")

	shop := got["shop"]
	require.Len(t, shop.Tables, 1, "vschema change must not become a table change")
	assert.Equal(t, "users", shop.Tables[0].Table)
	assert.Equal(t, "alter", shop.Tables[0].Operation)
	assert.Equal(t, `{"sharded":true}`, shop.Artifacts[vSchemaArtifactName])

	fallback := got["testapp"]
	require.Len(t, fallback.Tables, 1)
	assert.Equal(t, "orders", fallback.Tables[0].Table)
	assert.Equal(t, "create", fallback.Tables[0].Operation)
}

// A DDL-only request whose schema files still carry vschema.json (the common
// Vitess case) must not attach the vschema artifact, so Apply() does not create
// a spurious vschema_update task.
func TestNamespacesFromApplyRequest_DDLOnlyOmitsVSchemaArtifact(t *testing.T) {
	c := newPlanMaterializeClient(&fakePlanStore{})
	changes := []*ternv1.TableChange{
		{TableName: "users", Ddl: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Namespace: "shop"},
	}
	schemaFiles := schema.SchemaFiles{
		"shop": {Files: map[string]string{vSchemaArtifactName: `{"sharded":true}`}},
	}

	got, err := c.namespacesFromApplyRequest(changes, schemaFiles)
	require.NoError(t, err)

	require.Contains(t, got, "shop")
	assert.Empty(t, got["shop"].Artifacts[vSchemaArtifactName], "DDL-only request must not materialize a vschema artifact")
}

// A vschema change with no vschema.json artifact in the schema files fails
// closed rather than silently dropping the vschema update.
func TestNamespacesFromApplyRequest_VSchemaChangeWithoutArtifactFailsClosed(t *testing.T) {
	c := newPlanMaterializeClient(&fakePlanStore{})
	changes := []*ternv1.TableChange{
		{TableName: "VSchema: shop", ChangeType: ternv1.ChangeType_CHANGE_TYPE_VSCHEMA, Namespace: "shop"},
	}

	_, err := c.namespacesFromApplyRequest(changes, schema.SchemaFiles{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "vschema change")
}

// MySQL treats the literal "default" namespace as the database namespace, so a
// change carrying Namespace="default" is grouped under the database, consistent
// with planNamespace at plan time.
func TestNamespacesFromApplyRequest_DefaultNamespaceMapsToDatabase(t *testing.T) {
	c := newPlanMaterializeClient(&fakePlanStore{})
	changes := []*ternv1.TableChange{
		{TableName: "users", Ddl: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Namespace: "default"},
	}

	got, err := c.namespacesFromApplyRequest(changes, schema.SchemaFiles{})
	require.NoError(t, err)

	require.Contains(t, got, "testapp")
	assert.NotContains(t, got, "default")
	require.Len(t, got["testapp"].Tables, 1)
	assert.Equal(t, "users", got["testapp"].Tables[0].Table)
}

// An unmapped change type recovers a real operation from the request's
// authoritative DDL instead of persisting "unknown".
func TestNamespacesFromApplyRequest_UnmappedChangeTypeClassifiesDDL(t *testing.T) {
	c := newPlanMaterializeClient(&fakePlanStore{})
	changes := []*ternv1.TableChange{
		{TableName: "orders", Ddl: "CREATE TABLE `orders` (`id` bigint)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_OTHER, Namespace: ""},
	}

	got, err := c.namespacesFromApplyRequest(changes, schema.SchemaFiles{})
	require.NoError(t, err)

	require.Contains(t, got, "testapp")
	require.Len(t, got["testapp"].Tables, 1)
	assert.Equal(t, "create", got["testapp"].Tables[0].Operation)
}
