package api

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// storedPlans serves a plan or apply under test from capturingPlanStore and
// the plans of the applies already on the database by row ID.
type storedPlans struct {
	capturingPlanStore
	byID map[int64]*storage.Plan
}

func (s *storedPlans) GetByID(_ context.Context, id int64) (*storage.Plan, error) {
	return s.byID[id], nil
}

// applyAltering returns an apply in applyState on database in environment
// whose stored plan, planID, alters the given tables.
func applyAltering(applyID, applyState, database, environment string, rowID, planID int64, tables ...string) (*storage.Apply, *storage.Plan) {
	changes := make([]storage.TableChange, 0, len(tables))
	for _, table := range tables {
		changes = append(changes, storage.TableChange{
			Namespace: database, Table: table, Operation: "alter",
			DDL: "ALTER TABLE `" + table + "` ADD COLUMN `note` varchar(50)",
		})
	}
	return &storage.Apply{
			ID: rowID, ApplyIdentifier: applyID, PlanID: planID, State: applyState,
			Database: database, DatabaseType: storage.DatabaseTypeMySQL, Environment: environment, Deployment: DefaultDeployment,
		}, &storage.Plan{
			ID: planID, Database: database, DatabaseType: storage.DatabaseTypeMySQL, Environment: environment,
			Namespaces: map[string]*storage.NamespacePlanData{database: {Tables: changes}},
		}
}

// planWithDropTables plans payments in staging with the drop list, against the
// applies that have not settled there and the plans they run.
func planWithDropTables(t *testing.T, unsettled []*storage.Apply, existing map[int64]*storage.Plan, operations []*storage.ApplyOperation, dropTables ...string) (*mockTernClient, *storedPlans, error) {
	t.Helper()
	plans := &storedPlans{byID: existing}
	client := &mockTernClient{planResp: &ternv1.PlanResponse{PlanId: "plan-drops"}}
	svc := dropTablesService(plans, &unsettledApplyStore{unsettled: unsettled}, &staticApplyOperationStore{operations: operations}, client)
	_, err := svc.ExecutePlan(t.Context(), PlanRequest{
		Database:    "payments",
		Environment: "staging",
		Type:        storage.DatabaseTypeMySQL,
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"payments": {Files: map[string]string{"users.sql": "CREATE TABLE users (id bigint primary key)"}},
		},
		DropTables: dropTables,
	})
	return client, plans, err
}

// A stopped apply keeps its copy on the target so a start can resume it, and
// the engine cannot tell that copy from an abandoned one because nothing
// writes to it. A drop_tables entry naming one of its working tables is
// refused before anything is planned, naming the entry, the apply and its
// table, and what to do with the apply. A running apply's working tables are
// refused the same way. Spirit shortens a long table's working-table names to
// fit 64 characters, and the shortened name is the one protected.
func TestExecutePlanRefusesDropsOfAnUnsettledApplysWorkingTables(t *testing.T) {
	const longTable = "customer_payment_method_verification_attempts_by_region_daily_v2"
	for _, tc := range []struct {
		name       string
		applyState string
		tables     []string
		dropTables []string
		want       string
	}{
		{
			name: "the copy of a stopped apply", applyState: state.Apply.Stopped, tables: []string{"users"},
			dropTables: []string{"_events_old", "_users_new"},
			want:       `drop_tables entry "_users_new" is a working table of apply apply-7f3a, which is stopped and changes table "users". Dropping it would discard that apply's copy. Start or cancel it, then plan again.`,
		},
		{
			name: "the checkpoint of a stopped apply", applyState: state.Apply.Stopped, tables: []string{"users"},
			dropTables: []string{"_users_chkpnt"},
			want:       `drop_tables entry "_users_chkpnt" is a working table of apply apply-7f3a, which is stopped and changes table "users". Dropping it would discard that apply's copy. Start or cancel it, then plan again.`,
		},
		{
			name: "a working table of a running apply", applyState: state.Apply.Running, tables: []string{"users"},
			dropTables: []string{"_users_old"},
			want:       `drop_tables entry "_users_old" is a working table of apply apply-7f3a, which is running and changes table "users". Dropping it would discard that apply's copy. Wait for it to finish or cancel it, then plan again.`,
		},
		{
			name: "the shortened copy name of a 64-character table", applyState: state.Apply.Stopped, tables: []string{longTable},
			dropTables: []string{"_customer_payment_method_verification_attempts_by_region_dai_new"},
			want:       `drop_tables entry "_customer_payment_method_verification_attempts_by_region_dai_new" is a working table of apply apply-7f3a, which is stopped and changes table "` + longTable + `". Dropping it would discard that apply's copy. Start or cancel it, then plan again.`,
		},
		{
			name: "the checkpoint a stopped run over several tables shares", applyState: state.Apply.Stopped, tables: []string{"users", "orders"},
			dropTables: []string{"_spirit_checkpoint"},
			want:       `drop_tables entry "_spirit_checkpoint" is a working table of apply apply-7f3a, which is stopped and changes several tables in one run. Dropping it would discard that apply's copy. Start or cancel it, then plan again.`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			apply, plan := applyAltering("apply-7f3a", tc.applyState, "payments", "staging", 11, 7, tc.tables...)

			client, plans, err := planWithDropTables(t, []*storage.Apply{apply}, map[int64]*storage.Plan{7: plan}, nil, tc.dropTables...)

			require.Error(t, err)
			_, inUse := errors.AsType[*DropTablesInUseError](err)
			assert.True(t, inUse, "the refusal is typed so the API answers 409 and the PR comment shows it: %v", err)
			assert.EqualError(t, err, tc.want)
			assert.Nil(t, client.planReq, "nothing is planned for a drop the control plane refuses")
			assert.Nil(t, plans.created, "no plan is stored")
		})
	}
}

// Only the working tables of the applies that have not settled on the same
// database and environment are protected. A leftover of a table no such apply
// changes, the same names in another environment, and the shared checkpoint
// when no run changes several tables are all planned as drops.
func TestExecutePlanDropsTablesNoUnsettledApplyUses(t *testing.T) {
	for _, tc := range []struct {
		name        string
		environment string
		tables      []string
		dropTables  []string
	}{
		{name: "a leftover of a table the stopped apply does not change", environment: "staging", tables: []string{"orders"}, dropTables: []string{"_users_old", "_users_new"}},
		{name: "a stopped apply in another environment", environment: "production", tables: []string{"users"}, dropTables: []string{"_users_new"}},
		{name: "the shared checkpoint when the stopped run changes one table", environment: "staging", tables: []string{"users"}, dropTables: []string{"_spirit_checkpoint"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			apply, plan := applyAltering("apply-7f3a", state.Apply.Stopped, "payments", tc.environment, 11, 7, tc.tables...)

			client, plans, err := planWithDropTables(t, []*storage.Apply{apply}, map[int64]*storage.Plan{7: plan}, nil, tc.dropTables...)

			require.NoError(t, err)
			require.NotNil(t, client.planReq)
			assert.Equal(t, tc.dropTables, client.planReq.GetDropTables())
			assert.NotNil(t, plans.created)
		})
	}
}

// An apply of a rollout can run a member's own plan, stored on the member's
// operation. The tables that plan changes are protected as well, so a member
// target whose plan alone alters users keeps its copy.
func TestExecutePlanRefusesDropsOfAWorkingTableFromAnOperationsOwnPlan(t *testing.T) {
	apply, primaryPlan := applyAltering("apply-7f3a", state.Apply.Stopped, "payments", "staging", 11, 7, "orders")
	_, memberPlan := applyAltering("apply-7f3a", state.Apply.Stopped, "payments", "staging", 11, 9, "users")
	operations := []*storage.ApplyOperation{
		{ID: 1, ApplyID: 11, Deployment: DefaultDeployment},
		{ID: 2, ApplyID: 11, Deployment: "region-b", PlanID: 9},
	}

	client, _, err := planWithDropTables(t, []*storage.Apply{apply}, map[int64]*storage.Plan{7: primaryPlan, 9: memberPlan}, operations, "_users_new")

	require.Error(t, err)
	assert.Contains(t, err.Error(), `drop_tables entry "_users_new" is a working table of apply apply-7f3a, which is stopped and changes table "users"`)
	assert.Nil(t, client.planReq)
}

// Whether a stopped apply still needs a table can only be answered from
// storage. When storage cannot be read, the drop is refused rather than
// planned as though no apply needed it, and the error says which check failed.
func TestExecutePlanRefusesDropTablesWhenUnsettledAppliesCannotBeRead(t *testing.T) {
	plans := &storedPlans{}
	client := &mockTernClient{planResp: &ternv1.PlanResponse{PlanId: "plan-drops"}}
	svc := dropTablesService(plans, &unsettledApplyStore{err: errors.New("storage unavailable")}, nil, client)

	_, err := svc.ExecutePlan(t.Context(), PlanRequest{
		Database:    "payments",
		Environment: "staging",
		Type:        storage.DatabaseTypeMySQL,
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"payments": {Files: map[string]string{"users.sql": "CREATE TABLE users (id bigint primary key)"}},
		},
		DropTables: []string{"_users_old"},
	})

	require.EqualError(t, err, "check drop_tables for payments/staging against applies that may still resume: storage unavailable")
	_, inUse := errors.AsType[*DropTablesInUseError](err)
	assert.False(t, inUse, "a storage failure is a server error, not a refusal of the request")
	assert.Nil(t, client.planReq)
	assert.Nil(t, plans.created)
}

// Storage admits a new apply past a stopped one, so an apply can be stopped
// after a plan listing its copy was made. Apply creation runs the same check
// against the plan's recorded drop list and refuses before anything is stored.
func TestExecuteApplyRefusesDropOfAWorkingTableOfAnApplyStoppedAfterThePlan(t *testing.T) {
	plan := executeApplyTestPlan()
	plan.RecordDropTables([]string{"_users_new"})
	stopped, stoppedPlan := applyAltering("apply-7f3a", state.Apply.Stopped, "testdb", "staging", 11, 7, "users")
	created := &capturingApplyStore{}
	tasks := &capturingTaskStore{}
	created.taskStore = tasks
	svc := New(&mockStorageWithApplyStores{
		plans:     &staticPlanStore{plan: plan, plansByID: map[int64]*storage.Plan{7: stoppedPlan}},
		applies:   &unsettledApplyStore{ApplyStore: created, unsettled: []*storage.Apply{stopped}},
		tasks:     tasks,
		locks:     &emptyLockStore{},
		applyLogs: &noopApplyLogStore{},
	}, testServerConfig(), map[string]tern.Client{
		"default/staging": &mockTernClient{},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	resp, applyID, err := svc.ExecuteApply(t.Context(), ApplyRequest{PlanID: "plan-1", Environment: "staging"})

	require.EqualError(t, err, `drop_tables entry "_users_new" is a working table of apply apply-7f3a, which is stopped and changes table "users". Dropping it would discard that apply's copy. Start or cancel it, then plan again.`)
	assert.Nil(t, resp)
	assert.Zero(t, applyID)
	assert.Nil(t, created.apply, "no apply is stored")
	assert.Empty(t, tasks.tasks)
	assert.Equal(t, applyMetricStatusRejected, applyMetricStatusForError(err), "the refusal is the caller's to resolve, not a server error")
}
