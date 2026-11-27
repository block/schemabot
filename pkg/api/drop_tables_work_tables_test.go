package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// storedPlans serves a plan or apply under test from capturingPlanStore and
// the plans of the applies already on the database by row ID.
type storedPlans struct {
	capturingPlanStore
	byID    map[int64]*storage.Plan
	readErr error
}

func (s *storedPlans) GetByID(_ context.Context, id int64) (*storage.Plan, error) {
	if s.readErr != nil {
		return nil, s.readErr
	}
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

// Whether an apply that may still resume needs a table can only be answered
// from storage: the applies that have not settled, their operations, and the
// plans they run. When any of these cannot be read, or an apply names no plan,
// the drop is refused rather than planned as though the apply needed nothing,
// and the error says which read failed.
func TestExecutePlanRefusesDropTablesWhenWhatAnApplyChangesCannotBeRead(t *testing.T) {
	const prefix = "check drop_tables for payments/staging against applies that may still resume: "
	for _, tc := range []struct {
		name          string
		unsettledErr  error
		planID        int64
		stored        bool
		planReadErr   error
		operationsErr error
		want          string
	}{
		{name: "the unsettled applies cannot be listed", unsettledErr: errors.New("storage unavailable"), want: prefix + "storage unavailable"},
		{name: "the apply names no plan", want: prefix + "apply apply-7f3a: neither the apply nor its operations name a plan, so the tables the apply changes are unknown"},
		{name: "the named plan is not stored", planID: 7, want: prefix + "apply apply-7f3a: plan 7 is not stored, so the tables the apply changes are unknown"},
		{name: "the plan cannot be read", planID: 7, stored: true, planReadErr: errors.New("plan read failed"), want: prefix + "apply apply-7f3a: load plan 7: plan read failed"},
		{name: "the operations cannot be listed", planID: 7, stored: true, operationsErr: errors.New("operations unavailable"), want: prefix + "apply apply-7f3a: list operations: operations unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			apply, plan := applyAltering("apply-7f3a", state.Apply.Stopped, "payments", "staging", 11, tc.planID, "orders")
			plans := &storedPlans{byID: map[int64]*storage.Plan{}, readErr: tc.planReadErr}
			if tc.stored {
				plans.byID[tc.planID] = plan
			}
			client := &mockTernClient{planResp: &ternv1.PlanResponse{PlanId: "plan-drops"}}
			svc := dropTablesService(plans,
				&unsettledApplyStore{unsettled: []*storage.Apply{apply}, err: tc.unsettledErr},
				&staticApplyOperationStore{err: tc.operationsErr}, client)

			_, err := svc.ExecutePlan(t.Context(), PlanRequest{
				Database:    "payments",
				Environment: "staging",
				Type:        storage.DatabaseTypeMySQL,
				SchemaFiles: map[string]*ternv1.SchemaFiles{
					"payments": {Files: map[string]string{"users.sql": "CREATE TABLE users (id bigint primary key)"}},
				},
				DropTables: []string{"_users_old"},
			})

			require.EqualError(t, err, tc.want)
			_, checkFailed := errors.AsType[*DropTablesCheckError](err)
			assert.True(t, checkFailed, "a read failure is typed so a PR comment shows a fixed line instead of its cause")
			assert.Nil(t, client.planReq, "nothing is planned when what the apply changes is unknown")
			assert.Nil(t, plans.created)
		})
	}
}

// The API answers a drop of a working table with 409 and the active-apply
// code, naming the apply, so a client can tell a refusal it resolves from a
// server error. The plan and the apply handlers answer it the same way.
func TestHandlersAnswer409ForADropOfAnUnsettledApplysWorkingTable(t *testing.T) {
	t.Run("plan", func(t *testing.T) {
		apply, plan := applyAltering("apply-7f3a", state.Apply.Stopped, "payments", "staging", 11, 7, "users")
		client := &mockTernClient{planResp: &ternv1.PlanResponse{PlanId: "plan-drops"}}
		svc := dropTablesService(&storedPlans{byID: map[int64]*storage.Plan{7: plan}},
			&unsettledApplyStore{unsettled: []*storage.Apply{apply}}, nil, client)
		rec := postJSON(t, svc.handlePlan, "/api/plan", PlanRequest{
			Database:    "payments",
			Environment: "staging",
			Type:        storage.DatabaseTypeMySQL,
			SchemaFiles: map[string]*ternv1.SchemaFiles{
				"payments": {Files: map[string]string{"users.sql": "CREATE TABLE users (id bigint primary key)"}},
			},
			DropTables: []string{"_users_new"},
		})

		assert.Equal(t, http.StatusConflict, rec.Code)
		assert.Contains(t, rec.Body.String(), apitypes.ErrCodeActiveApplyExists)
		assert.Contains(t, rec.Body.String(), "plan rejected: drop_tables entry \\\"_users_new\\\" is a working table of apply apply-7f3a")
		assert.Nil(t, client.planReq)
	})
	t.Run("apply", func(t *testing.T) {
		svc, created, _ := applyServiceWithAStoppedApplyOnUsers()
		rec := postJSON(t, svc.handleApply, "/api/apply", ApplyRequest{PlanID: "plan-1", Environment: "staging"})

		assert.Equal(t, http.StatusConflict, rec.Code)
		assert.Contains(t, rec.Body.String(), apitypes.ErrCodeActiveApplyExists)
		assert.Contains(t, rec.Body.String(), "apply rejected: drop_tables entry \\\"_users_new\\\" is a working table of apply apply-7f3a")
		assert.Nil(t, created.apply)
	})
}

// postJSON sends body to handler as a JSON POST to path and returns the
// recorded response.
func postJSON(t *testing.T, handler http.HandlerFunc, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	encoded, err := json.Marshal(body)
	require.NoError(t, err)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, bytes.NewReader(encoded))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler(rec, req)
	return rec
}

// A table listed under both drop_tables and ignore_tables is refused before
// any data plane is asked to plan. The engine refuses the overlap too, but a
// data plane that predates drop_tables reads only ignore_tables and would
// withhold the table without a word, leaving the requested drop unplanned and
// unreported.
func TestPlanRefusesATableListedUnderDropAndIgnoreTables(t *testing.T) {
	request := PlanRequest{
		Database:    "payments",
		Environment: "staging",
		Type:        storage.DatabaseTypeMySQL,
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"payments": {Files: map[string]string{"users.sql": "CREATE TABLE users (id bigint primary key)"}},
		},
		IgnoreTables: []string{"orders_archive_2024"},
		DropTables:   []string{"orders_archive_2024"},
	}
	const want = `drop_tables entry "orders_archive_2024" is also withheld by ignore_tables. Remove it from one of the two lists`

	t.Run("API", func(t *testing.T) {
		client := &mockTernClient{planResp: &ternv1.PlanResponse{PlanId: "plan-unused"}}
		rec := postJSON(t, withheldDropService(&capturingPlanStore{}, client).handlePlan, "/api/plan", request)

		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), `drop_tables entry \"orders_archive_2024\" is also withheld by ignore_tables`)
		assert.Nil(t, client.planReq)
	})
	t.Run("pull request", func(t *testing.T) {
		client := &mockTernClient{planResp: &ternv1.PlanResponse{PlanId: "plan-unused"}}
		_, err := withheldDropService(&capturingPlanStore{}, client).ExecutePlan(t.Context(), request)

		require.EqualError(t, err, want)
		assert.Nil(t, client.planReq)
	})
}

// Spirit's sentinel table holds an apply started with a deferred cutover until
// the table is dropped, so a drop_tables entry naming it would run that
// cutover without anyone asking. The entry is refused before any data plane is
// asked to plan, whatever its case, on both paths that accept the list.
func TestPlanRefusesADropOfTheCutoverSentinel(t *testing.T) {
	for _, entry := range []string{"_spirit_sentinel", "_SPIRIT_SENTINEL"} {
		request := PlanRequest{
			Database:    "payments",
			Environment: "staging",
			Type:        storage.DatabaseTypeMySQL,
			SchemaFiles: map[string]*ternv1.SchemaFiles{
				"payments": {Files: map[string]string{"users.sql": "CREATE TABLE users (id bigint primary key)"}},
			},
			DropTables: []string{"_users_old", entry},
		}
		want := `drop_tables entry "` + entry + "\" is the table that holds a deferred cutover until someone runs it, so dropping it would run that cutover. Remove the entry, and run the cutover with `schemabot cutover <apply-id> -e <env>` when you are ready"

		t.Run("API "+entry, func(t *testing.T) {
			client := &mockTernClient{planResp: &ternv1.PlanResponse{PlanId: "plan-unused"}}
			rec := postJSON(t, withheldDropService(&capturingPlanStore{}, client).handlePlan, "/api/plan", request)

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), `drop_tables entry \"`+entry+`\" is the table that holds a deferred cutover`)
			assert.Nil(t, client.planReq)
		})
		t.Run("pull request "+entry, func(t *testing.T) {
			client := &mockTernClient{planResp: &ternv1.PlanResponse{PlanId: "plan-unused"}}
			_, err := withheldDropService(&capturingPlanStore{}, client).ExecutePlan(t.Context(), request)

			require.EqualError(t, err, want)
			assert.Nil(t, client.planReq)
		})
	}
}

// A plan stored by an older server can carry a drop_tables list naming the
// cutover sentinel. Apply creation holds the stored list to the same rule a
// new plan is, so applying that plan by its id is refused as a blocked plan
// and nothing is stored.
func TestExecuteApplyRefusesAStoredPlanThatDropsTheCutoverSentinel(t *testing.T) {
	svc, created, tasks := applyServiceWithAStoppedApplyOnUsers()
	stored := executeApplyTestPlan()
	stored.RecordDropTables([]string{"_SPIRIT_SENTINEL"})
	svc.storage.(*mockStorageWithApplyStores).plans.(*staticPlanStore).plan = stored

	resp, applyID, err := svc.ExecuteApply(t.Context(), ApplyRequest{PlanID: "plan-1", Environment: "staging"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), `drop_tables entry "_SPIRIT_SENTINEL" is the table that holds a deferred cutover`)
	_, blocked := errors.AsType[*blockedPlanError](err)
	assert.True(t, blocked, "the refusal answers as a blocked plan: %v", err)
	assert.Nil(t, resp)
	assert.Zero(t, applyID)
	assert.Nil(t, created.apply, "no apply is stored")
	assert.Empty(t, tasks.tasks)
	assert.Equal(t, applyMetricStatusRejected, applyMetricStatusForError(err), "the refusal is the caller's to resolve, not a server error")
}

// A data plane that predates drop_tables plans as though the list were empty:
// the listed leftover stays, disclosed as a table skipped by its name. The
// plan is kept, because that is the outcome the repository had before it
// asked, and the server logs the entries it did not drop so an operator can
// tell why the drop is missing.
func TestExecutePlanLogsDropTablesEntriesAnOlderDataPlaneIgnored(t *testing.T) {
	plans := &capturingPlanStore{}
	client := &mockTernClient{planResp: &ternv1.PlanResponse{
		PlanId: "plan-old-data-plane",
		ExemptTables: []*ternv1.ExemptTables{
			{Namespace: "payments", Tables: []string{"_orders_old"}, Reason: engine.ExemptReasonLeadingUnderscore},
		},
	}}
	svc := withheldDropService(plans, client)
	var logs bytes.Buffer
	svc.logger = slog.New(slog.NewTextHandler(&logs, nil))

	_, err := svc.ExecutePlan(t.Context(), PlanRequest{
		Database:    "payments",
		Environment: "staging",
		Type:        storage.DatabaseTypeMySQL,
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"payments": {Files: map[string]string{"users.sql": "CREATE TABLE users (id bigint primary key)"}},
		},
		DropTables: []string{"_orders_old"},
	})

	require.NoError(t, err)
	assert.NotNil(t, plans.created, "the plan is kept")
	assert.Contains(t, logs.String(), "plan does not drop drop_tables entries that the planner still skips by their names")
	assert.Contains(t, logs.String(), "ignored_entries=[_orders_old]")
}

// applyServiceWithAStoppedApplyOnUsers wires a service whose stored plan-1
// for testdb in staging lists _users_new under drop_tables, while apply-7f3a,
// which alters users, is stopped there. created captures an apply if one is
// ever stored.
func applyServiceWithAStoppedApplyOnUsers() (*Service, *capturingApplyStore, *capturingTaskStore) {
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
	return svc, created, tasks
}

// Storage admits a new apply past a stopped one, so an apply can be stopped
// after a plan listing its copy was made. Apply creation runs the same check
// against the plan's recorded drop list and refuses before anything is stored.
func TestExecuteApplyRefusesDropOfAWorkingTableOfAnApplyStoppedAfterThePlan(t *testing.T) {
	svc, created, tasks := applyServiceWithAStoppedApplyOnUsers()

	resp, applyID, err := svc.ExecuteApply(t.Context(), ApplyRequest{PlanID: "plan-1", Environment: "staging"})

	require.EqualError(t, err, `drop_tables entry "_users_new" is a working table of apply apply-7f3a, which is stopped and changes table "users". Dropping it would discard that apply's copy. Start or cancel it, then plan again.`)
	assert.Nil(t, resp)
	assert.Zero(t, applyID)
	assert.Nil(t, created.apply, "no apply is stored")
	assert.Empty(t, tasks.tasks)
	assert.Equal(t, applyMetricStatusRejected, applyMetricStatusForError(err), "the refusal is the caller's to resolve, not a server error")
}
