package api

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

const targetNamespacesConfig = `
tern_deployments:
  eu:
    production: tern-eu:9090
databases:
  orders:
    type: mysql
    environments:
      production:
        deployment: eu
        targets:
          - orders-001
          - target: orders-002
            namespaces: [ns_2, ns_1]
          - target: orders-003
`

// A targets entry is a bare target name or a mapping that selects which
// declared namespaces live on the target. Members resolve in entry order and
// each keeps its namespaces in the order they were written, which is the
// fan-out order and the display order. A bare entry and a mapping without
// namespaces both select nothing, so the target covers every declared
// namespace.
func TestParseServerConfig_TargetEntriesSelectNamespaces(t *testing.T) {
	cfg, err := ParseServerConfig([]byte(targetNamespacesConfig))
	require.NoError(t, err)

	got, err := cfg.ResolveDatabaseTargets("orders", "production")
	require.NoError(t, err)
	assert.Equal(t, []routing.ExecutionTarget{
		{DatabaseType: "mysql", Deployment: "eu", Target: "orders-001"},
		{DatabaseType: "mysql", Deployment: "eu", Target: "orders-002", Namespaces: []string{"ns_2", "ns_1"}},
		{DatabaseType: "mysql", Deployment: "eu", Target: "orders-003"},
	}, got)
	assert.Equal(t, "eu/orders-002", got[1].MemberID(), "the target stays the member; its namespaces are not part of its identity")
}

// The strict decoder's unknown-field check does not reach a custom
// unmarshaler, so a misspelled key inside a mapping entry must still fail the
// load. Accepting it would leave the target silently covering every namespace.
func TestParseServerConfig_TargetEntryRejectsUnknownFields(t *testing.T) {
	for name, entry := range map[string]string{
		"misspelled namespaces": "- target: orders-002\n            namespace: [ns_1]",
		"unrelated key":         "- target: orders-002\n            dsn: root@tcp(db)/",
	} {
		t.Run(name, func(t *testing.T) {
			config := `
tern_deployments:
  eu:
    production: tern-eu:9090
databases:
  orders:
    type: mysql
    environments:
      production:
        deployment: eu
        targets:
          ` + entry + `
`
			_, err := ParseServerConfig([]byte(config))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "not found in targets entry")
		})
	}

	_, err := ParseServerConfig([]byte(`
tern_deployments:
  eu:
    production: tern-eu:9090
databases:
  orders:
    type: mysql
    environments:
      production:
        deployment: eu
        targets:
          - [orders-001]
`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "a targets entry must be a target name or a mapping")
}

// A namespaces key that is present but holds no list decodes to the same nil
// slice as an absent key. Loading it would read the author's selection as
// "every declared namespace", including namespaces other targets hold, so the
// load fails instead. An explicitly empty list is still a list, and is refused
// by validation as selecting nothing.
func TestParseServerConfig_TargetEntryRejectsNamespacesKeyWithoutList(t *testing.T) {
	for name, namespaces := range map[string]string{
		"no value":            "namespaces:",
		"explicit null":       "namespaces: ~",
		"null keyword":        "namespaces: null",
		"items commented out": "namespaces:\n              # - ns_1",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ParseServerConfig([]byte(`
tern_deployments:
  eu:
    production: tern-eu:9090
databases:
  orders:
    type: mysql
    environments:
      production:
        deployment: eu
        targets:
          - target: orders-001
            namespaces: [ns_0]
          - target: orders-002
            ` + namespaces + `
`))
			require.Error(t, err)
			assert.Contains(t, err.Error(), `targets entry "orders-002" has a namespaces key with no list`)
		})
	}
}

// A selection is an enumerated list of names, each checked where the config
// loads: empty, repeated, and delimiter-bearing names are refused, as is a
// target listed twice even with different selections, since the target is
// still the rollout member.
func TestServerConfig_ValidateTargetNamespaces(t *testing.T) {
	cases := []struct {
		name       string
		targets    []TargetEntry
		wantErrSub string
	}{
		{
			name:    "a selection on one target is accepted",
			targets: []TargetEntry{{Target: "orders-001", Namespaces: []string{"ns_0"}}, {Target: "orders-002"}},
		},
		{
			name:       "an explicitly empty selection is rejected",
			targets:    []TargetEntry{{Target: "orders-001", Namespaces: []string{}}},
			wantErrSub: `targets entry 0 "orders-001" namespaces list is empty`,
		},
		{
			name:       "an empty namespace name is rejected",
			targets:    []TargetEntry{{Target: "orders-001", Namespaces: []string{"ns_0", " "}}},
			wantErrSub: `targets entry 0 "orders-001" namespaces entry 1 is empty`,
		},
		{
			name:       "a namespace listed twice is rejected",
			targets:    []TargetEntry{{Target: "orders-001", Namespaces: []string{"ns_0", "ns_0"}}},
			wantErrSub: `lists namespace "ns_0" more than once`,
		},
		{
			name:       "a namespace containing the operation key delimiter is rejected",
			targets:    []TargetEntry{{Target: "orders-001", Namespaces: []string{"ns/0"}}},
			wantErrSub: `namespaces entry 0 "ns/0" contains reserved delimiter "/"`,
		},
		{
			name: "a target listed twice with different selections is rejected",
			targets: []TargetEntry{
				{Target: "orders-001", Namespaces: []string{"ns_0"}},
				{Target: "orders-001", Namespaces: []string{"ns_1"}},
			},
			wantErrSub: `lists target "orders-001" more than once`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := ServerConfig{
				Databases: map[string]DatabaseConfig{
					"orders": {Type: storage.DatabaseTypeMySQL, Environments: map[string]EnvironmentConfig{
						"production": {Deployment: "eu", Targets: tc.targets},
					}},
				},
				TernDeployments: TernConfig{"eu": {"production": "tern-eu:9090"}},
			}
			err := cfg.Validate()
			if tc.wantErrSub == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErrSub)
		})
	}
}

func threeNamespaceRequest() PlanRequest {
	files := func(table string) *ternv1.SchemaFiles {
		return &ternv1.SchemaFiles{Files: map[string]string{table + ".sql": "CREATE TABLE `" + table + "` (id bigint primary key)"}}
	}
	return PlanRequest{
		Database:    "orders",
		Environment: "production",
		Type:        storage.DatabaseTypeMySQL,
		SchemaFiles: map[string]*ternv1.SchemaFiles{"ns_0": files("orders"), "ns_1": files("orders"), "ns_2": files("orders")},
	}
}

// The schema files declare the namespace set and a selection only narrows it.
// A namespace the files do not declare, or one ignore_namespaces withholds, is
// an error naming the target and the namespace rather than a plan that
// silently leaves it out.
func TestMemberSchemaFiles(t *testing.T) {
	req := threeNamespaceRequest()

	all, err := memberSchemaFiles(req, routing.ExecutionTarget{Target: "orders-001"})
	require.NoError(t, err)
	assert.Equal(t, []string{"ns_0", "ns_1", "ns_2"}, slices.Sorted(maps.Keys(all)), "a target selecting nothing covers every declared namespace")

	selected, err := memberSchemaFiles(req, routing.ExecutionTarget{Target: "orders-002", Namespaces: []string{"ns_2", "ns_1"}})
	require.NoError(t, err)
	assert.Equal(t, []string{"ns_1", "ns_2"}, slices.Sorted(maps.Keys(selected)))
	assert.Same(t, req.SchemaFiles["ns_1"], selected["ns_1"])
	assert.Len(t, req.SchemaFiles, 3, "narrowing one member must not narrow the request the others are planned from")

	_, err = memberSchemaFiles(req, routing.ExecutionTarget{Target: "orders-002", Namespaces: []string{"ns_1", "ns_9"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `target "orders-002" selects namespace "ns_9", which the schema files do not declare (declared: ns_0, ns_1, ns_2)`)

	req.IgnoredNamespaces = []string{"ns_3"}
	_, err = memberSchemaFiles(req, routing.ExecutionTarget{Target: "orders-002", Namespaces: []string{"ns_3"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `target "orders-002" selects namespace "ns_3", which ignore_namespaces withholds`)
}

func namespaceSelectionService(t *testing.T, client *mockTernClient, plans storage.PlanStore) *Service {
	t.Helper()
	return namespaceSelectionServiceWith(t, client, plans, []TargetEntry{
		{Target: "orders-001", Namespaces: []string{"ns_0"}},
		{Target: "orders-002", Namespaces: []string{"ns_1", "ns_2"}},
	})
}

func namespaceSelectionServiceWith(t *testing.T, client *mockTernClient, plans storage.PlanStore, targets []TargetEntry) *Service {
	t.Helper()
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"orders": {Type: storage.DatabaseTypeMySQL, Environments: map[string]EnvironmentConfig{
				"production": {Deployment: "eu", Targets: targets},
			}},
		},
		TernDeployments: TernConfig{"eu": {"production": "tern-eu:9090"}},
	}
	return New(&mockStorageWithPlanLookup{plans: plans}, cfg, map[string]tern.Client{"eu/production": client},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// The primary plans only the namespaces its entry selects, and its stored plan
// records only those, so the apply created from it has nothing to run on a
// namespace another target holds.
func TestExecutePlan_PrimaryPlansOnlyItsSelectedNamespaces(t *testing.T) {
	client := &mockTernClient{isRemote: true, planResp: &ternv1.PlanResponse{PlanId: "plan-primary"}}
	plans := &capturingPlanStore{}
	svc := namespaceSelectionService(t, client, plans)

	resp, err := svc.ExecutePlan(t.Context(), threeNamespaceRequest())
	require.NoError(t, err)
	assert.Equal(t, []string{"ns_0"}, resp.SelectedNamespaces, "the response records the selection the rollup checks the primary against")

	require.NotNil(t, client.planReq)
	assert.Equal(t, "orders-001", client.planReq.Target)
	assert.Equal(t, []string{"ns_0"}, slices.Sorted(maps.Keys(client.planReq.SchemaFiles)))
	assert.Equal(t, []string{"ns_1", "ns_2"}, client.planReq.UnselectedNamespaces, "the data plane is told which declared namespaces the narrowed files withhold")
	require.NotNil(t, plans.created)
	assert.Equal(t, []string{"ns_0"}, slices.Sorted(maps.Keys(plans.created.SchemaFiles)))
}

// A primary whose entry selects a namespace the schema files do not declare
// fails the plan before anything is planned or stored, rather than planning the
// declared part of its selection and reporting it clean.
func TestExecutePlan_PrimaryUndeclaredSelectionFails(t *testing.T) {
	client := &mockTernClient{isRemote: true, planResp: &ternv1.PlanResponse{PlanId: "plan-primary"}}
	plans := &capturingPlanStore{}
	svc := namespaceSelectionServiceWith(t, client, plans, []TargetEntry{
		{Target: "orders-001", Namespaces: []string{"ns_0", "ns_9"}},
		{Target: "orders-002", Namespaces: []string{"ns_1", "ns_2"}},
	})

	_, err := svc.ExecutePlan(t.Context(), threeNamespaceRequest())
	require.Error(t, err)
	assert.Contains(t, err.Error(), `target "orders-001" selects namespace "ns_9", which the schema files do not declare`)
	assert.True(t, NamespacePlacementRefused(err), "the refusal is typed so callers can fail a merge gate and answer 400 on it")
	assert.Nil(t, client.planReq, "nothing is planned for an undeclared selection")
	assert.Nil(t, plans.created, "no plan is stored for an undeclared selection")
}

// Namespace coverage holds for every plan of the environment, not only the pull
// request review. A lone target whose entry selects a subset of the declared
// namespaces would otherwise plan, store, and report a clean plan while the
// namespaces no entry selects are planned nowhere.
func TestExecutePlan_UncoveredNamespaceBlocks(t *testing.T) {
	client := &mockTernClient{isRemote: true, planResp: &ternv1.PlanResponse{PlanId: "plan-primary"}}
	plans := &capturingPlanStore{}
	svc := namespaceSelectionServiceWith(t, client, plans, []TargetEntry{
		{Target: "orders-001", Namespaces: []string{"ns_0"}},
	})

	_, err := svc.ExecutePlan(t.Context(), threeNamespaceRequest())
	require.Error(t, err)
	assert.Contains(t, err.Error(), `database "orders" environment "production" declares namespaces [ns_1, ns_2] that no targets entry selects`)
	assert.True(t, NamespacePlacementRefused(err), "the refusal is typed so callers can fail a merge gate and answer 400 on it")
	assert.Nil(t, client.planReq, "nothing is planned while a namespace is unplaced")
	assert.Nil(t, plans.created, "no plan is stored while a namespace is unplaced")
}

// An uncovered namespace is a defect in the caller's schema files or the
// server's config, so POST /api/plan answers it as a bad request naming the
// namespaces rather than as a server failure.
func TestPlanHandler_UncoveredNamespaceIsBadRequest(t *testing.T) {
	client := &mockTernClient{isRemote: true, planResp: &ternv1.PlanResponse{PlanId: "plan-primary"}}
	svc := namespaceSelectionServiceWith(t, client, &capturingPlanStore{}, []TargetEntry{
		{Target: "orders-001", Namespaces: []string{"ns_0"}},
	})
	planReq := threeNamespaceRequest()
	planReq.RendersRollout = true
	body, err := json.Marshal(planReq)
	require.NoError(t, err)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/plan", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	svc.handlePlan(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), `declares namespaces [ns_1, ns_2] that no targets entry selects`)
	assert.Nil(t, client.planReq, "nothing is planned while a namespace is unplaced")
}

// A non-primary member is diffed against, and stores, only its own selection.
// A member selecting an undeclared namespace blocks the review rather than
// reporting a clean plan for a namespace no schema file describes.
func TestRollupReviewTimeDrift_MemberPlansOnlyItsSelectedNamespaces(t *testing.T) {
	primary := routing.ExecutionTarget{Deployment: "eu", Target: "orders-001", Namespaces: []string{"ns_0"}}
	reviewed := &ternv1.PlanResponse{PlanId: "plan-primary", Engine: ternv1.Engine_ENGINE_SPIRIT}

	t.Run("declared selection", func(t *testing.T) {
		client := &mockTernClient{planDiffResp: &ternv1.PlanDiffResponse{Engine: ternv1.Engine_ENGINE_SPIRIT}}
		plans := &recordingPlanStore{}
		svc := namespaceSelectionService(t, client, plans)

		rollup, err := svc.RollupReviewTimeDrift(t.Context(), threeNamespaceRequest(), reviewed, primary)
		require.NoError(t, err)
		require.Len(t, rollup.Entries, 2)
		assert.Equal(t, DeploymentPlanned, rollup.Entries[1].Class)

		require.NotNil(t, client.planDiffReq)
		assert.Equal(t, "orders-002", client.planDiffReq.Target)
		assert.Equal(t, []string{"ns_1", "ns_2"}, slices.Sorted(maps.Keys(client.planDiffReq.SchemaFiles)))
		assert.Equal(t, []string{"ns_0"}, client.planDiffReq.UnselectedNamespaces, "the data plane is told which declared namespaces the narrowed files withhold")
		require.Len(t, plans.created, 1)
		assert.Equal(t, []string{"ns_1", "ns_2"}, slices.Sorted(maps.Keys(plans.created[0].SchemaFiles)))
	})

	t.Run("undeclared selection", func(t *testing.T) {
		client := &mockTernClient{planDiffResp: &ternv1.PlanDiffResponse{Engine: ternv1.Engine_ENGINE_SPIRIT}}
		plans := &recordingPlanStore{}
		svc := namespaceSelectionService(t, client, plans)
		req := threeNamespaceRequest()
		delete(req.SchemaFiles, "ns_2")

		rollup, err := svc.RollupReviewTimeDrift(t.Context(), req, reviewed, primary)
		require.NoError(t, err)
		assert.False(t, rollup.Clean)
		require.Len(t, rollup.Entries, 2)
		assert.Equal(t, DeploymentErrored, rollup.Entries[1].Class)
		assert.ErrorContains(t, rollup.Entries[1].Err, `target "orders-002" selects namespace "ns_2"`)
		assert.Nil(t, client.planDiffReq, "a member with an undeclared selection is never diffed")
		assert.Empty(t, plans.created)
	})
}

// A declared namespace is covered when some member selects it, and a member
// selecting nothing holds every declared namespace, so its presence covers the
// whole set.
func TestUncoveredNamespaces(t *testing.T) {
	req := threeNamespaceRequest()
	req.SchemaFiles["ns_3"] = req.SchemaFiles["ns_0"]

	assert.Empty(t, uncoveredNamespaces(req, []routing.ExecutionTarget{
		{Target: "orders-001", Namespaces: []string{"ns_0"}},
		{Target: "orders-002"},
	}), "a member selecting nothing covers every declared namespace")
	assert.Equal(t, []string{"ns_1", "ns_3"}, uncoveredNamespaces(req, []routing.ExecutionTarget{
		{Target: "orders-001", Namespaces: []string{"ns_0"}},
		{Target: "orders-002", Namespaces: []string{"ns_2"}},
	}))
}

// The reviewed primary plan covers the namespaces its entry selected when it
// was planned. A primary member reported with a different selection for the
// same target would pair that plan with members placed under another one, so
// the rollup fails closed rather than trusting the target name alone.
func TestRollupReviewTimeDrift_PrimarySelectionChangedFailsClosed(t *testing.T) {
	plannedUnder := routing.ExecutionTarget{Deployment: "eu", Target: "orders-001", Namespaces: []string{"ns_0", "ns_1"}}
	reviewed := &ternv1.PlanResponse{PlanId: "plan-primary", Engine: ternv1.Engine_ENGINE_SPIRIT}
	client := &mockTernClient{planDiffResp: &ternv1.PlanDiffResponse{Engine: ternv1.Engine_ENGINE_SPIRIT}}
	plans := &recordingPlanStore{}
	svc := namespaceSelectionService(t, client, plans)

	_, err := svc.RollupReviewTimeDrift(t.Context(), threeNamespaceRequest(), reviewed, plannedUnder)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rollout member eu/orders-001 now selects namespaces [ns_0] but the reviewed plan was created for [ns_0, ns_1]")
	assert.Nil(t, client.planDiffReq)
	assert.Empty(t, plans.created)
}

// A pull of every namespace asks a selecting target for its selection by name,
// so the data plane never scans the cluster for it; a target selecting nothing
// is still asked to discover its own. An explicitly requested namespace a
// target does not select is left out of that target's pull.
func TestMemberPullNamespaces(t *testing.T) {
	svc := &Service{logger: slog.New(slog.DiscardHandler)}
	req := pullRequest()
	selecting := routing.ExecutionTarget{Deployment: "eu", Target: "orders-002", Namespaces: []string{"ns_2", "ns_1"}}

	assert.Equal(t, []string{""}, svc.memberPullNamespaces(req, routing.ExecutionTarget{Target: "orders-001"}, []string{""}))
	assert.Equal(t, []string{"ns_0"}, svc.memberPullNamespaces(req, routing.ExecutionTarget{Target: "orders-001"}, []string{"ns_0"}))
	assert.Equal(t, []string{"ns_2", "ns_1"}, svc.memberPullNamespaces(req, selecting, []string{""}))
	assert.Equal(t, []string{"ns_1"}, svc.memberPullNamespaces(req, selecting, []string{"ns_0", "ns_1"}))
}

// A target holding several namespaces can hold the same table in more than one
// of them. Unsharded work is one operation per target, keyed by the target, and
// each table's task carries its namespace, so two namespaces' orders tables are
// two distinct tasks under one operation and no key collides.
func TestBuildApplyOperationGroups_SameTableInTwoNamespacesOfOneTarget(t *testing.T) {
	alter := "ALTER TABLE `orders` ADD COLUMN `note` varchar(64)"
	applyPlan := primaryPlanRow("orders-001")
	applyPlan.Namespaces = map[string]*storage.NamespacePlanData{
		"ns_0": {Tables: []storage.TableChange{{Namespace: "ns_0", Table: "orders", DDL: alter, Operation: "alter"}}},
		"ns_1": {Tables: []storage.TableChange{{Namespace: "ns_1", Table: "orders", DDL: alter, Operation: "alter"}}},
	}
	secondPlan := &storage.Plan{ID: 11, Deployment: "eu", Target: "orders-002", Namespaces: map[string]*storage.NamespacePlanData{
		"ns_2": {Tables: []storage.TableChange{{Namespace: "ns_2", Table: "orders", DDL: alter, Operation: "alter"}}},
	}}
	members := []applyMember{
		{Target: routing.ExecutionTarget{Deployment: "eu", Target: "orders-001", Namespaces: []string{"ns_0", "ns_1"}}, Plan: applyPlan},
		{Target: routing.ExecutionTarget{Deployment: "eu", Target: "orders-002", Namespaces: []string{"ns_2"}}, Plan: secondPlan},
	}

	groups, sharded, err := buildApplyOperationGroups(applyPlan, applyTaskChanges(applyPlan), members, "production", storage.ApplyOptions{}, "", "", pershardTestTime())
	require.NoError(t, err)
	assert.False(t, sharded)
	require.Len(t, groups, 2)

	assert.Equal(t, "orders-001", groups[0].Operation.OperationKey)
	assert.Equal(t, "orders-002", groups[1].Operation.OperationKey)

	tasks := make([]string, 0, len(groups[0].Tasks))
	for _, task := range groups[0].Tasks {
		tasks = append(tasks, task.Namespace+"."+task.TableName)
	}
	slices.Sort(tasks)
	assert.Equal(t, []string{"ns_0.orders", "ns_1.orders"}, tasks)
	require.Len(t, groups[1].Tasks, 1)
	assert.Equal(t, "ns_2", groups[1].Tasks[0].Namespace)
}

// A pull request whose source the database's policy denies is reported as a
// source policy block even when its schema files also leave a namespace
// unplaced, so the refusal, its metric and its log name the check that denied
// it rather than the placement defect behind it.
func TestExecutePlan_SourcePolicyDenialPrecedesNamespacePlacement(t *testing.T) {
	client := &mockTernClient{isRemote: true, planResp: &ternv1.PlanResponse{PlanId: "plan-primary"}}
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"orders": {
				Type:         storage.DatabaseTypeMySQL,
				AllowedRepos: []string{"octocat/orders-schema"},
				Environments: map[string]EnvironmentConfig{
					"production": {Deployment: "eu", Targets: []TargetEntry{{Target: "orders-001", Namespaces: []string{"ns_0"}}}},
				},
			},
		},
		TernDeployments: TernConfig{"eu": {"production": "tern-eu:9090"}},
	}
	svc := New(&mockStorageWithPlanLookup{plans: &capturingPlanStore{}}, cfg, map[string]tern.Client{"eu/production": client},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	req := threeNamespaceRequest()
	req.SourceTrusted = true
	req.Repository = "octocat/other-repo"
	req.PullRequest = new(int32(7))
	req.SchemaPath = "schema/orders"

	_, err := svc.ExecutePlan(t.Context(), req)
	require.Error(t, err)
	var policyErr *SourcePolicyError
	require.ErrorAs(t, err, &policyErr)
	assert.Equal(t, SourcePolicyReasonUnauthorizedRepo, policyErr.Reason)
	assert.False(t, NamespacePlacementRefused(err), "the denial is not reported as the placement defect the files also carry")
	assert.Nil(t, client.planReq)
}

// When every targets entry selects namespaces, a pull naming one none of them
// selects is a bad request naming it and the selectable ones, not an empty
// schema that reads as a namespace holding no tables. An entry selecting
// nothing holds every namespace, so there any name is pulled as requested.
func TestPullHandler_UnselectedNamespaceIsBadRequest(t *testing.T) {
	client := &mockTernClient{isRemote: true}
	svc := namespaceSelectionService(t, client, &capturingPlanStore{})
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/pull",
		bytes.NewReader([]byte(`{"database":"orders","environment":"production","namespaces":["ns_1","ns_l"]}`)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), `has no targets entry selecting namespaces [ns_l]; the selectable namespaces are [ns_0, ns_1, ns_2]`)
	assert.Empty(t, client.pullSchemaReqs, "nothing is pulled for a namespace no target holds")

	assert.NoError(t, requireSelectablePullNamespaces(pullRequest(), []routing.ExecutionTarget{
		{Target: "orders-001", Namespaces: []string{"ns_0"}},
		{Target: "orders-002"},
	}, []string{"ns_l"}), "an entry selecting nothing may hold any namespace")
	assert.NoError(t, requireSelectablePullNamespaces(pullRequest(), []routing.ExecutionTarget{
		{Target: "orders-001", Namespaces: []string{"ns_0"}},
	}, []string{""}), "a pull of every namespace names none to check")
}

// A plan requested through the API for a rollout whose primary selects
// namespaces plans the other members beside it: the primary is held to the
// selection it was planned under, which the plan response records, so the
// rollout comes back with every member rather than failing the primary check
// against an empty selection.
func TestHandlePlan_RolloutWithNamespaceSelectingPrimaryPlansEveryMember(t *testing.T) {
	client := &mockTernClient{
		isRemote:     true,
		planResp:     &ternv1.PlanResponse{PlanId: "plan-primary", Engine: ternv1.Engine_ENGINE_SPIRIT},
		planDiffResp: &ternv1.PlanDiffResponse{Engine: ternv1.Engine_ENGINE_SPIRIT},
	}
	plans := &recordingPlanStore{}
	svc := namespaceSelectionService(t, client, plans)

	planReq := threeNamespaceRequest()
	planReq.RendersRollout = true
	body, err := json.Marshal(planReq)
	require.NoError(t, err)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/plan", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	svc.handlePlan(w, req)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var resp apitypes.PlanResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, []string{"ns_0"}, resp.SelectedNamespaces)
	require.NotNil(t, resp.Rollout, "the rollout is planned beside the namespace-selecting primary")
	assert.Equal(t, 2, resp.Rollout.Members)
	assert.Empty(t, resp.Rollout.Attention)

	require.NotNil(t, client.planDiffReq)
	assert.Equal(t, "orders-002", client.planDiffReq.Target)
	assert.Equal(t, []string{"ns_1", "ns_2"}, slices.Sorted(maps.Keys(client.planDiffReq.SchemaFiles)))
	require.Len(t, plans.created, 2, "the primary's plan and the other member's plan are both stored")
	assert.Equal(t, "orders-002", plans.created[1].Target)
	assert.Equal(t, "plan-primary", plans.created[1].PrimaryPlanIdentifier)
}
