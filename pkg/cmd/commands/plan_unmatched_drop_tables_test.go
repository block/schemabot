package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/presentation"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

const cliUnmatchedDropLine = `⚠️  drop_tables entry "_jobs_olds" names no table on the target, so this plan drops nothing for it. Fix the name, or remove the entry if the table is already gone.`

// unmatchedDropTablesTernClient answers a plan the way the MySQL engine does
// for a drop_tables list with one entry naming a live leftover and one with a
// typo: the leftover is planned as a drop, and the typo is reported as naming
// no table.
type unmatchedDropTablesTernClient struct {
	tern.Client
}

func (c *unmatchedDropTablesTernClient) Plan(context.Context, *ternv1.PlanRequest) (*ternv1.PlanResponse, error) {
	return &ternv1.PlanResponse{
		PlanId: "plan-drop-tables",
		Engine: ternv1.Engine_ENGINE_SPIRIT,
		Changes: []*ternv1.SchemaChange{{
			Namespace: "testdb",
			TableChanges: []*ternv1.TableChange{{
				TableName:    "_jobs_old",
				Ddl:          "DROP TABLE `_jobs_old`",
				ChangeType:   ternv1.ChangeType_CHANGE_TYPE_DROP,
				IsUnsafe:     true,
				UnsafeReason: "DROP TABLE removes all data",
			}},
		}},
		UnmatchedDropTables: []string{"_jobs_olds"},
	}, nil
}

func (c *unmatchedDropTablesTernClient) IsRemote() bool   { return true }
func (c *unmatchedDropTablesTernClient) Endpoint() string { return "remote:9090" }

// planOnlyStorage stores plans without keeping them and holds no apply that
// could still resume, and freezes, which a plan reads to name. Only the calls
// a plan request makes are implemented.
type planOnlyStorage struct {
	storage.Storage
	freezes []*storage.EnvironmentFreeze
}

func (planOnlyStorage) Plans() storage.PlanStore    { return discardingPlanStore{} }
func (planOnlyStorage) Applies() storage.ApplyStore { return noUnsettledApplyStore{} }
func (s planOnlyStorage) Freezes() storage.FreezeStore {
	return listedFreezeStore{current: s.freezes}
}

// listedFreezeStore lists the freezes it holds as current, in effect or
// scheduled.
type listedFreezeStore struct {
	storage.FreezeStore
	current []*storage.EnvironmentFreeze
}

func (s listedFreezeStore) List(context.Context, string, int) (*storage.FreezeListing, error) {
	return &storage.FreezeListing{Now: time.Now(), Current: s.current}, nil
}

type discardingPlanStore struct {
	storage.PlanStore
}

func (discardingPlanStore) Create(context.Context, *storage.Plan) (int64, error) { return 1, nil }

type noUnsettledApplyStore struct {
	storage.ApplyStore
}

func (noUnsettledApplyStore) GetUnsettled(context.Context) ([]*storage.Apply, error) { return nil, nil }

// A repository lists two leftovers under drop_tables, one spelled right and
// one with a typo. `schemabot plan` shows the drop of the first and says the
// second names no table and drops nothing, the same sentence the PR plan
// comment carries. The CLI talks to the server's own plan endpoint, so the
// output shows what the server actually returns.
func TestPlanCmd_SaysWhichDropTablesEntryNamesNoTable(t *testing.T) {
	cfg := &api.ServerConfig{
		Databases: map[string]api.DatabaseConfig{
			"testdb": {Type: storage.DatabaseTypeMySQL, Environments: map[string]api.EnvironmentConfig{
				"staging": {Deployment: "remote", Target: "testdb"},
			}},
		},
		TernDeployments: api.TernConfig{"remote": {"staging": "remote:9090"}},
	}
	svc := api.New(planOnlyStorage{}, cfg, map[string]tern.Client{"remote/staging": &unmatchedDropTablesTernClient{}}, slog.New(slog.DiscardHandler))
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	dir := writeTestSchemaDir(t)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "schemabot.yaml"),
		[]byte("database: testdb\ntype: mysql\ndrop_tables:\n  - _jobs_old\n  - _jobs_olds\n"), 0o644))

	cmd := PlanCmd{SchemaDir: dir, Environment: "staging"}
	var runErr error
	out := stripAnsi(captureStdout(func() { runErr = cmd.Run(&Globals{Endpoint: server.URL}) }))
	require.NoError(t, runErr, out)

	assert.Contains(t, out, "DROP TABLE `_jobs_old`", out)
	assert.Contains(t, out, cliUnmatchedDropLine+"\n", out)
	assert.NotContains(t, out, `entry "_jobs_old" names no table`, "the entry that planned a drop is not reported")
}

// `schemabot plan` of a frozen database says, before anyone applies, that
// the apply would be refused now, and names a scheduled freeze with when it
// starts, as the PR plan comment does. The CLI talks to the server's own plan
// endpoint, so the output shows what the server actually returns.
func TestPlanCmd_NamesTheFreezes(t *testing.T) {
	cfg := &api.ServerConfig{
		Databases: map[string]api.DatabaseConfig{
			"testdb": {Type: storage.DatabaseTypeMySQL, Environments: map[string]api.EnvironmentConfig{
				"staging": {Deployment: "remote", Target: "testdb"},
			}},
		},
		TernDeployments: api.TernConfig{"remote": {"staging": "remote:9090"}},
	}
	startsAt := time.Now().Add(24 * time.Hour).UTC().Truncate(time.Second)
	endsAt := startsAt.Add(24 * time.Hour)
	store := planOnlyStorage{freezes: []*storage.EnvironmentFreeze{
		{Identifier: "freeze-0000000000000001", Environment: "staging", Kind: storage.FreezeKindOnDemand,
			StartsAt: time.Now().Add(-time.Hour), Reason: "incident 42"},
		{Identifier: "freeze-0000000000000002", Environment: "staging", Databases: []string{"testdb"}, Kind: storage.FreezeKindPlanned,
			StartsAt: startsAt, EndsAt: &endsAt, Reason: "launch week"},
		{Identifier: "freeze-0000000000000003", Environment: "staging", Databases: []string{"otherdb"}, Kind: storage.FreezeKindOnDemand,
			StartsAt: time.Now().Add(-time.Hour), Reason: "another database"},
	}}
	svc := api.New(store, cfg, map[string]tern.Client{"remote/staging": &unmatchedDropTablesTernClient{}}, slog.New(slog.DiscardHandler))
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	dir := writeTestSchemaDir(t)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "schemabot.yaml"),
		[]byte("database: testdb\ntype: mysql\ndrop_tables:\n  - _jobs_old\n"), 0o644))

	cmd := PlanCmd{SchemaDir: dir, Environment: "staging"}
	var runErr error
	out := stripAnsi(captureStdout(func() { runErr = cmd.Run(&Globals{Endpoint: server.URL}) }))
	require.NoError(t, runErr, out)

	assert.Contains(t, out, "DROP TABLE `_jobs_old`", out)
	assert.Contains(t, out, "❄️ staging is frozen, so applying this plan would be refused now.\n"+
		"   freeze-0000000000000001  incident 42  (until lifted)\n"+
		`Wait for the freeze to end before you apply. If this change can't wait, apply with --override "<reason>": as the server's override policy says, SchemaBot lets it through on your reason, or asks a freeze approver to approve it first.`+"\n", out)
	assert.Contains(t, out, "❄️ A freeze is scheduled in staging.", out)
	assert.Contains(t, out, "   freeze-0000000000000002  launch week  (starts "+startsAt.Format(time.RFC3339)+", until "+endsAt.Format(time.RFC3339)+")", out)
	assert.NotContains(t, out, "another database", "a freeze of another database is not named")
}

// A plan that changes nothing offers no apply, so it names no freeze; a plan
// shown once for every environment names each environment's own freezes.
func TestOutputMultiEnvPlanResult_Freezes(t *testing.T) {
	frozen := []apitypes.Freeze{{ID: "freeze-0000000000000001", Reason: "incident 42", InEffect: true}}
	drop := &apitypes.TableChangeResponse{TableName: "_jobs_old", DDL: "DROP TABLE `_jobs_old`;", ChangeType: "drop"}

	t.Run("no changes", func(t *testing.T) {
		out := stripAnsi(captureStdout(func() {
			outputMultiEnvPlanResult(map[string]*apitypes.PlanResponse{"staging": {Engine: "spirit", Freezes: frozen}}, "testdb", "testdb")
		}))
		assert.NotContains(t, out, "❄️", out)
	})

	t.Run("a plan apply refuses", func(t *testing.T) {
		blocked := planWithTablesAndEngine("spirit", &apitypes.TableChangeResponse{
			TableName: "_jobs_old", DDL: "DROP TABLE `_jobs_old`;", ChangeType: "drop",
			ExecutionMode: engine.ExecutionModeBlocked, ModeReason: "direct execution is disabled",
		})
		blocked.Freezes = frozen
		require.NotEmpty(t, planRefusal(blocked), "the fixture is a plan apply refuses")
		out := stripAnsi(captureStdout(func() {
			outputMultiEnvPlanResult(map[string]*apitypes.PlanResponse{"staging": blocked}, "testdb", "testdb")
		}))
		assert.NotContains(t, out, "❄️", out)
	})

	t.Run("one plan for every environment", func(t *testing.T) {
		staging := planWithTablesAndEngine("spirit", drop)
		production := planWithTablesAndEngine("spirit", drop)
		production.Freezes = frozen
		out := stripAnsi(captureStdout(func() {
			outputMultiEnvPlanResult(map[string]*apitypes.PlanResponse{"staging": staging, "production": production}, "testdb", "testdb")
		}))
		assert.Contains(t, out, "Staging & Production", "the plans render as one section:\n%s", out)
		assert.Contains(t, out, "❄️ production is frozen", out)
		assert.NotContains(t, out, "❄️ staging", out)
	})
}

// The plan body reports an entry that names no table on both of its branches,
// in the plan and apply layouts: under the summary when there are changes,
// and under the no-changes message when there are none, which is where a
// typo would otherwise pass for a clean plan.
func TestWritePlanBody_UnmatchedDropTablesReported(t *testing.T) {
	withChanges := planWithTablesAndEngine("spirit", &apitypes.TableChangeResponse{
		TableName: "_jobs_old", DDL: "DROP TABLE `_jobs_old`;", ChangeType: "drop", IsUnsafe: true, UnsafeReason: "DROP TABLE removes all data",
	})
	withChanges.UnmatchedDropTables = []string{"_jobs_olds"}
	noChanges := &apitypes.PlanResponse{Engine: "spirit", UnmatchedDropTables: []string{"_jobs_olds"}}

	for name, isApply := range map[string]bool{"plan": false, "apply": true} {
		t.Run(name, func(t *testing.T) {
			out := stripAnsi(captureStdout(func() { writePlanBody(withChanges, isApply) }))
			summaryAt := strings.Index(out, "📋 Plan: 1 table to drop")
			lineAt := strings.Index(out, cliUnmatchedDropLine)
			assert.Greater(t, lineAt, summaryAt, "the line follows the summary:\n%s", out)

			out = stripAnsi(captureStdout(func() { writePlanBody(noChanges, isApply) }))
			noChangesAt := strings.Index(out, "No schema changes detected")
			lineAt = strings.Index(out, cliUnmatchedDropLine)
			assert.Greater(t, lineAt, noChangesAt, "the line follows the no-changes message:\n%s", out)
		})
	}
}

// Staging has already applied the drop of _jobs_old, so its plan is clean and
// reports the entry as naming no table, while production still has the table
// and plans the drop. The output shows production's drop and does not tell the
// reader to remove the entry that drop depends on; the typo both environments
// report stays under each section.
func TestOutputMultiEnvPlanResult_UnmatchedDropTablesNotReportedBesideAnotherEnvironmentsDrop(t *testing.T) {
	staging := &apitypes.PlanResponse{Engine: "spirit", UnmatchedDropTables: []string{"_jobs_old", "_jobs_olds"}}
	production := planWithTablesAndEngine("spirit", &apitypes.TableChangeResponse{
		TableName: "_jobs_old", DDL: "DROP TABLE `_jobs_old`;", ChangeType: "drop", IsUnsafe: true, UnsafeReason: "DROP TABLE removes all data",
	})
	production.UnmatchedDropTables = []string{"_jobs_olds"}

	out := stripAnsi(captureStdout(func() {
		outputMultiEnvPlanResult(map[string]*apitypes.PlanResponse{"staging": staging, "production": production}, "testdb", "testdb")
	}))

	assert.Contains(t, out, "DROP TABLE `_jobs_old`", out)
	assert.NotContains(t, out, `entry "_jobs_old" names no table`, out)
	assert.Equal(t, 2, strings.Count(out, cliUnmatchedDropLine+"\n"), out)
}

// Apply against a target with nothing to reconcile returns before the plan
// body, so it reports an entry that names no table itself, once.
func TestApplyCmd_UnmatchedDropTablesReportedOnCleanPlan(t *testing.T) {
	plan := &apitypes.PlanResponse{Engine: "mysql", UnmatchedDropTables: []string{"_jobs_olds"}}
	body, err := json.Marshal(plan)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/api/plan" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, writeErr := w.Write(body)
		require.NoError(t, writeErr)
	}))
	t.Cleanup(server.Close)

	cmd := ApplyCmd{SchemaDir: writeTestSchemaDir(t), Environment: "staging", NoLock: true}
	out := stripAnsi(captureStdout(func() { _ = cmd.Run(t.Context(), &Globals{Endpoint: server.URL}) }))

	assert.Contains(t, out, "No changes. Your schema is up-to-date.", out)
	assert.Equal(t, 1, strings.Count(out, cliUnmatchedDropLine+"\n"), out)
}

// dropJobsOld is the DROP of the leftover _jobs_old a target still has.
func dropJobsOld() []*apitypes.SchemaChangeResponse {
	return []*apitypes.SchemaChangeResponse{{
		Namespace: "orders",
		TableChanges: []*apitypes.TableChangeResponse{{
			TableName: "_jobs_old", Namespace: "orders", DDL: "DROP TABLE `_jobs_old`", ChangeType: "drop",
			IsUnsafe: true, UnsafeReason: "DROP TABLE removes all data",
		}},
	}}
}

// A rollout plan reports no unmatched drop_tables entry: the primary's entries
// say nothing about the other targets, and here the other target drops the
// table the primary reports as missing. The output shows that target's DROP
// and no line telling the reader to remove the entry it depends on, in the
// plan layout, the apply layout, and a multi-environment plan.
func TestWritePlanBody_RolloutReportsNoUnmatchedDropTables(t *testing.T) {
	rollout := func() *apitypes.PlanResponse {
		return &apitypes.PlanResponse{
			Database: "orders",
			Engine:   "spirit",
			Changes:  []*apitypes.SchemaChangeResponse{},
			Rollout: &apitypes.PlanRolloutResponse{
				Members:     2,
				Independent: true,
				Groups: []*apitypes.PlanMemberGroupResponse{
					{Members: paymentsTargets(1, 1), Primary: true, Changes: []*apitypes.SchemaChangeResponse{}},
					{Members: paymentsTargets(2, 2), Changes: dropJobsOld()},
				},
			},
			UnmatchedDropTables: []string{"_jobs_old", "_jobs_olds"},
		}
	}

	for name, isApply := range map[string]bool{"plan": false, "apply": true} {
		t.Run(name, func(t *testing.T) {
			out := stripAnsi(captureStdout(func() { writePlanBody(rollout(), isApply) }))
			assert.Contains(t, out, "DROP TABLE `_jobs_old`", out)
			assert.NotContains(t, out, "names no table", out)
		})
	}

	t.Run("multi-environment", func(t *testing.T) {
		staging := &apitypes.PlanResponse{Engine: "spirit", UnmatchedDropTables: []string{"_jobs_olds"}}
		out := stripAnsi(captureStdout(func() {
			outputMultiEnvPlanResult(map[string]*apitypes.PlanResponse{"staging": staging, "production": rollout()}, "orders", "orders")
		}))
		assert.Contains(t, out, "DROP TABLE `_jobs_old`", out)
		assert.NotContains(t, out, "names no table", out)
	})
}

// Apply against a rollout with nothing to reconcile on any target returns
// before the plan body, and like the body it reports no unmatched entry.
func TestApplyCmd_RolloutReportsNoUnmatchedDropTablesOnCleanPlan(t *testing.T) {
	plan := &apitypes.PlanResponse{
		Engine:  "mysql",
		Changes: []*apitypes.SchemaChangeResponse{},
		Rollout: &apitypes.PlanRolloutResponse{
			Members:     2,
			Independent: true,
			Groups:      []*apitypes.PlanMemberGroupResponse{{Members: paymentsTargets(1, 2), Primary: true, Changes: []*apitypes.SchemaChangeResponse{}}},
		},
		UnmatchedDropTables: []string{"_jobs_olds"},
	}
	body, err := json.Marshal(plan)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/api/plan" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, writeErr := w.Write(body)
		require.NoError(t, writeErr)
	}))
	t.Cleanup(server.Close)

	cmd := ApplyCmd{SchemaDir: writeTestSchemaDir(t), Environment: "staging", NoLock: true}
	out := stripAnsi(captureStdout(func() { _ = cmd.Run(t.Context(), &Globals{Endpoint: server.URL}) }))

	assert.Contains(t, out, "No changes. Your schema is up-to-date.", out)
	assert.NotContains(t, out, "names no table", out)
}

// A drop_tables list with many entries that name no table names the first
// ones and counts the rest, the same entries the PR comment names.
func TestWritePlanBody_ManyUnmatchedDropTablesAreCounted(t *testing.T) {
	plan := &apitypes.PlanResponse{Engine: "spirit"}
	for i := range 400 {
		plan.UnmatchedDropTables = append(plan.UnmatchedDropTables, fmt.Sprintf("_jobs_old_%04d", i))
	}

	out := stripAnsi(captureStdout(func() { writePlanBody(plan, false) }))
	assert.Equal(t, presentation.UnmatchedDropTablesListLimit, strings.Count(out, "names no table on the target, so this plan drops nothing for it."), out)
	assert.Contains(t, out, `drop_tables entry "_jobs_old_0019" names no table`)
	assert.NotContains(t, out, "_jobs_old_0020")
	assert.Contains(t, out, "⚠️  380 more drop_tables entries name no table on the target, so this plan drops nothing for them. Fix the names, or remove the entries whose tables are already gone.\n", out)
}
