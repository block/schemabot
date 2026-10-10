package tern

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
)

// fakePlanStore lets a test script the plan Get/Create behavior the plan
// materialization path depends on.
type fakePlanStore struct {
	storage.PlanStore
	getFn     func(planIdentifier string) (*storage.Plan, error)
	getByIDFn func(id int64) (*storage.Plan, error)
	createID  int64
	createErr error
	created   *storage.Plan
}

func (f *fakePlanStore) Get(_ context.Context, planIdentifier string) (*storage.Plan, error) {
	return f.getFn(planIdentifier)
}

func (f *fakePlanStore) GetByID(_ context.Context, id int64) (*storage.Plan, error) {
	return f.getByIDFn(id)
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

func (e fakePlanEngine) Name() string { return "spirit" }

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
				Operation: ddl.StatementAlterTable,
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
		{TableName: "users", Ddl: "ALTER TABLE `users` DROP COLUMN `legacy_id`", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Namespace: "shop", IsUnsafe: true, UnsafeReason: "DROP COLUMN removes data"},
		{TableName: "orders", Ddl: "CREATE TABLE `orders` (`id` bigint)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_CREATE, Namespace: ""},
		{TableName: "VSchema: shop", ChangeType: ternv1.ChangeType_CHANGE_TYPE_VSCHEMA, Namespace: "shop"},
	}
	schemaFiles := schema.SchemaFiles{
		"shop": {Files: map[string]string{storage.VSchemaArtifactName: `{"sharded":true}`}},
	}

	got, err := c.namespacesFromApplyRequest(changes, schemaFiles)
	require.NoError(t, err)

	require.Contains(t, got, "shop")
	require.Contains(t, got, "testapp")

	shop := got["shop"]
	require.Len(t, shop.Tables, 1, "vschema change must not become a table change")
	assert.Equal(t, "users", shop.Tables[0].Table)
	assert.Equal(t, "alter", shop.Tables[0].Operation)
	assert.True(t, shop.Tables[0].IsUnsafe)
	assert.Equal(t, "DROP COLUMN removes data", shop.Tables[0].UnsafeReason)
	assert.Equal(t, `{"sharded":true}`, shop.Artifacts[storage.VSchemaArtifactName])

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
		"shop": {Files: map[string]string{storage.VSchemaArtifactName: `{"sharded":true}`}},
	}

	got, err := c.namespacesFromApplyRequest(changes, schemaFiles)
	require.NoError(t, err)

	require.Contains(t, got, "shop")
	assert.Empty(t, got["shop"].Artifacts[storage.VSchemaArtifactName], "DDL-only request must not materialize a vschema artifact")
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

// A deployment materializes dispatched changes with its own dialect's parser: a
// PostgreSQL deployment recovers the operation for an unmapped change type from
// Postgres-only DDL that the MySQL-family grammar cannot parse.
func TestNamespacesFromApplyRequest_UnmappedChangeTypeUsesTargetDialectParser(t *testing.T) {
	c := newPlanMaterializeClient(&fakePlanStore{})
	c.config.Type = storage.DatabaseTypePostgres
	changes := []*ternv1.TableChange{
		{TableName: "orders", Ddl: "CREATE TABLE orders (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_OTHER, Namespace: ""},
	}

	got, err := c.namespacesFromApplyRequest(changes, schema.SchemaFiles{})
	require.NoError(t, err)

	require.Contains(t, got, "testapp")
	require.Len(t, got["testapp"].Tables, 1)
	assert.Equal(t, "create", got["testapp"].Tables[0].Operation)
}

// The materialized plan is where this deployment's task rows come from, so the
// statement it stores is the statement the engine will be asked to run. Once
// the drift comparison has proven the dispatched change and this deployment's
// re-plan the same under canonicalization, the stored statement is the
// re-plan's — the text this target's own engine emitted — not the primary's.
func TestMaterializedPlanCarriesLocalReplannedStatement(t *testing.T) {
	recomputed := alterUsersEmailPlan()
	recomputed.Changes[0].TableChanges[0].DDL = "ALTER TABLE users ADD COLUMN email varchar(255)"
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }, createID: 31}
	c := newPlanMaterializeClientWithPlan(store, recomputed)

	got, err := c.planForApplyRequest(t.Context(), alterUsersEmailDispatch("plan_local_statement"))

	require.NoError(t, err)
	require.NotNil(t, store.created)
	change := store.created.Namespaces["testapp"].Tables[0]
	assert.Equal(t, "ALTER TABLE users ADD COLUMN email varchar(255)", change.DDL)
	flat := got.FlatDDLChanges()
	require.Len(t, flat, 1)
	assert.Equal(t, "ALTER TABLE users ADD COLUMN email varchar(255)", flat[0].DDL, "task rows are built from the flattened plan")
}

// A namespace can map to a differently named physical schema on each
// deployment, and a PostgreSQL engine qualifies every statement it plans with
// the schema it planned against. The dispatched text therefore names the
// primary's schema, which this target does not have; the materialized plan
// carries this target's re-planned statement, qualified with its own schema,
// so the apply names the schema it preflighted.
func TestMaterializedPostgresPlanNamesThisDeploymentsSchema(t *testing.T) {
	const dispatched = `ALTER TABLE "svc-database-qa".users ADD COLUMN email text`
	const local = `ALTER TABLE "svc-database-us-qa".users ADD COLUMN email text`
	fake := &namespaceCaptureEngine{planResult: &engine.PlanResult{Changes: []engine.SchemaChange{{
		Namespace: "svc-database-us-qa",
		TableChanges: []engine.TableChange{{
			Table:         "users",
			Operation:     ddl.StatementAlterTable,
			DDL:           local,
			ExecutionMode: engine.ExecutionModeDirect,
			ModeReason:    "metadata-only change",
		}},
	}}}}
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }, createID: 32}
	c := postgresOverrideClient(fake)
	c.config.SchemaOverrides = map[string]string{"svc": "svc-database-us-qa"}
	c.storage = &fakePlanStorage{plans: store}

	got, err := c.planForApplyRequest(t.Context(), &ternv1.ApplyRequest{
		PlanId:      "plan_pg_schema",
		Environment: "qa",
		DdlChanges: []*ternv1.TableChange{
			{TableName: "users", Ddl: dispatched, ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Namespace: "svc"},
		},
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"svc": {Files: map[string]string{"users.sql": "CREATE TABLE users (id bigint, email text);"}},
		},
	})

	require.NoError(t, err)
	require.NotNil(t, store.created)
	assert.Contains(t, fake.planRequest.SchemaFiles, "svc-database-us-qa", "the re-plan runs against this target's physical schema")
	ns := store.created.Namespaces["svc"]
	require.NotNil(t, ns)
	require.Len(t, ns.Tables, 1)
	assert.Equal(t, local, ns.Tables[0].DDL)
	assert.Equal(t, "svc", ns.Tables[0].Namespace, "the stored namespace stays canonical; only the statement is this target's")
	assert.Equal(t, engine.ExecutionModeDirect, ns.Tables[0].ExecutionMode)
	flat := got.FlatDDLChanges()
	require.Len(t, flat, 1)
	assert.Equal(t, local, flat[0].DDL)
}

// A dispatched change that already arrives blocked is never run here, so it
// keeps the reviewed text it was refused with rather than taking a statement
// from a re-plan that would have run it.
func TestMaterializedPlanKeepsDispatchedTextForBlockedChange(t *testing.T) {
	recomputed := alterUsersEmailPlan()
	recomputed.Changes[0].TableChanges[0].DDL = "ALTER TABLE users ADD COLUMN email varchar(255)"
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }, createID: 33}
	c := newPlanMaterializeClientWithPlan(store, recomputed)
	req := alterUsersEmailDispatch("plan_blocked_text")
	req.DdlChanges[0].ExecutionMode = engine.ExecutionModeBlocked
	req.DdlChanges[0].ModeReason = "the primary's engine refuses this statement"

	_, err := c.planForApplyRequest(t.Context(), req)

	require.NoError(t, err)
	change := store.created.Namespaces["testapp"].Tables[0]
	assert.Equal(t, "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", change.DDL)
	assert.Equal(t, engine.ExecutionModeBlocked, change.ExecutionMode)
}

// A shard-scoped dispatch builds its task rows from the dispatched changes
// rather than from the plan row, so the statement this deployment's re-plan
// produced has to be carried onto the dispatch scope as well: the task rows
// and the plan row of one apply must hold the same statement and verdict.
func TestShardScopedDispatchScopeCarriesLocalReplannedStatement(t *testing.T) {
	const local = "ALTER TABLE users ADD COLUMN email varchar(255)"
	recomputed := alterUsersEmailShardPlan("-80")
	recomputed.Changes[0].TableChanges[0].DDL = local
	recomputed.Changes[0].TableChanges[0].ExecutionMode = engine.ExecutionModeDirect
	recomputed.Changes[0].TableChanges[0].ModeReason = "metadata-only change"
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }, createID: 34}
	c := newPlanMaterializeClientWithPlan(store, recomputed)
	req := alterUsersEmailDispatch("plan_shard_local_statement", "-80")

	plan, err := c.planForApplyRequest(t.Context(), req)
	require.NoError(t, err)
	require.NotNil(t, plan)
	assert.Equal(t, local, plan.Namespaces["testapp"].Tables[0].DDL)

	scope, err := c.dispatchScopeForApply(plan, req)
	require.NoError(t, err)
	assert.Equal(t, "-80", scope.shard)
	require.Len(t, scope.ddlChanges, 1)
	assert.Equal(t, local, scope.ddlChanges[0].DDL, "the task row runs the same statement the plan row stores")
	assert.Equal(t, engine.ExecutionModeDirect, scope.ddlChanges[0].ExecutionMode)
	assert.Equal(t, "metadata-only change", scope.ddlChanges[0].ModeReason)
}

// A dispatched change with no counterpart in the plan row keeps the dispatched
// statement: the plan row was materialized by a sibling dispatch of the same
// plan and holds only that dispatch's changes, so the text this dispatch
// carries is the one it was reviewed and dispatched with.
func TestShardScopedDispatchScopeKeepsDispatchedTextWithoutCounterpart(t *testing.T) {
	const dispatched = "ALTER TABLE `orders` ADD COLUMN `note` varchar(255)"
	plan := &storage.Plan{
		ID:             35,
		PlanIdentifier: "plan_shard_sibling",
		Namespaces: map[string]*storage.NamespacePlanData{"testapp": {Tables: []storage.TableChange{
			{Namespace: "testapp", Table: "users", Operation: "alter", DDL: "ALTER TABLE users ADD COLUMN email varchar(255)"},
		}}},
	}
	c := newPlanMaterializeClient(&fakePlanStore{})
	req := &ternv1.ApplyRequest{
		PlanId:       "plan_shard_sibling",
		TargetShards: []string{"80-"},
		DdlChanges: []*ternv1.TableChange{
			{TableName: "orders", Ddl: dispatched, ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Namespace: "testapp"},
		},
	}

	scope, err := c.dispatchScopeForApply(plan, req)
	require.NoError(t, err)
	require.Len(t, scope.ddlChanges, 1)
	assert.Equal(t, dispatched, scope.ddlChanges[0].DDL)
	assert.Empty(t, scope.ddlChanges[0].ExecutionMode)
}

// A whole-deployment dispatch already builds its scope from the plan row, so
// the stamped statement reaches the task rows without substitution.
func TestWholeDeploymentDispatchScopeIsThePlanRow(t *testing.T) {
	const local = "ALTER TABLE users ADD COLUMN email varchar(255)"
	recomputed := alterUsersEmailPlan()
	recomputed.Changes[0].TableChanges[0].DDL = local
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }, createID: 36}
	c := newPlanMaterializeClientWithPlan(store, recomputed)
	req := alterUsersEmailDispatch("plan_whole_local_statement")

	plan, err := c.planForApplyRequest(t.Context(), req)
	require.NoError(t, err)

	scope, err := c.dispatchScopeForApply(plan, req)
	require.NoError(t, err)
	assert.Empty(t, scope.shard)
	require.Len(t, scope.ddlChanges, 1)
	assert.Equal(t, local, scope.ddlChanges[0].DDL)
}
