package api

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// A rollout plan made through the API lists the targets whose own plans a
// rollout-wide apply of it refuses, so the CLI can refuse before it locks and
// prompts. The list is what apply creation refuses, member for member: each
// listed target is refused by createStoredApply under the unsafe opt-in, and a
// rollout with nothing listed is applied.
func TestRolloutApplyRefusals_ListWhatApplyCreationRefuses(t *testing.T) {
	alter := storage.TableChange{Namespace: "testapp", Table: "users", Operation: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"}
	drop := storage.TableChange{Namespace: "testapp", Table: "legacy_orders", Operation: "drop", DDL: "DROP TABLE `legacy_orders`", IsUnsafe: true}
	direct := alter
	direct.ExecutionMode = "direct"
	direct.ModeReason = "table is 12 MiB, within the direct execution bound"
	blocked := alter
	blocked.ExecutionMode = "blocked"
	reviewedWith := func(changes ...storage.TableChange) *storage.Plan {
		plan := primaryPlanRow("testapp-001")
		plan.Environment = "production"
		if len(changes) > 0 {
			plan.Namespaces = map[string]*storage.NamespacePlanData{"testapp": {Tables: changes}}
		}
		return plan
	}
	memberWith := func(changes ...storage.TableChange) *storage.Plan {
		plan := memberPlanRow("plan-second", "testapp-002", "plan-primary")
		plan.Namespaces = map[string]*storage.NamespacePlanData{"testapp": {Tables: changes}}
		return plan
	}

	for _, tc := range []struct {
		name     string
		reviewed *storage.Plan
		member   *storage.Plan
		// want is the refusal listed for testapp-002, nil when none is.
		want *apitypes.PlanMemberRefusalResponse
	}{
		{name: "same work as the reviewed plan", reviewed: reviewedWith(alter), member: memberWith(alter)},
		{name: "unsafe change the reviewed plan with work does not carry", reviewed: reviewedWith(alter), member: memberWith(alter, drop),
			want: &apitypes.PlanMemberRefusalResponse{Member: "eu/testapp-002", Target: "testapp-002", Reason: apitypes.PlanMemberNeedsTarget,
				Detail: `carries an unsafe change for table "legacy_orders" that the reviewed plan does not carry`, AllowUnsafe: true}},
		{name: "unsafe change beside a converged reviewed target", reviewed: reviewedWith(), member: memberWith(drop),
			want: &apitypes.PlanMemberRefusalResponse{Member: "eu/testapp-002", Target: "testapp-002", Reason: apitypes.PlanMemberNeedsTarget,
				Detail: `carries an unsafe change for table "legacy_orders" that the reviewed plan does not carry`, AllowUnsafe: true}},
		{name: "direct-execution change beside reviewed work", reviewed: reviewedWith(alter), member: memberWith(direct),
			want: &apitypes.PlanMemberRefusalResponse{Member: "eu/testapp-002", Target: "testapp-002", Reason: apitypes.PlanMemberNeedsTarget,
				Detail: `runs table "users" as direct-execution DDL, which a rollout-wide apply runs only from the pull request comment that discloses it under this target`}},
		{name: "direct-execution change beside a converged reviewed target", reviewed: reviewedWith(), member: memberWith(direct),
			want: &apitypes.PlanMemberRefusalResponse{Member: "eu/testapp-002", Target: "testapp-002", Reason: apitypes.PlanMemberNeedsTarget,
				Detail: `runs table "users" as direct-execution DDL, which a rollout-wide apply runs only from the pull request comment that discloses it under this target`}},
		{name: "blocked change", reviewed: reviewedWith(alter), member: memberWith(blocked),
			want: &apitypes.PlanMemberRefusalResponse{Member: "eu/testapp-002", Target: "testapp-002", Reason: apitypes.PlanMemberBlocked,
				Detail: "carries changes its target's engine refuses"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := multiTargetApplyService(t, &listingPlanStore{
				mockPlanLookupStore: mockPlanLookupStore{plan: tc.reviewed},
				plans:               []*storage.Plan{tc.member},
			})

			refused, err := svc.rolloutApplyRefusals(t.Context(), "production", "plan-primary", targetsFor(t, svc))
			require.NoError(t, err)

			_, _, applyErr := svc.createStoredApply(t.Context(), tc.reviewed, ApplyRequest{Environment: "production"},
				map[string]string{"allow_unsafe": "true"}, "apply-rollout-refusals")
			if tc.want == nil {
				assert.Empty(t, refused)
				require.NoError(t, applyErr, "apply creation runs a rollout that lists no refusal")
				return
			}
			require.Len(t, refused, 1)
			assert.Equal(t, tc.want, refused[0])
			require.Error(t, applyErr, "apply creation refuses the target the rollout lists")
			assert.Contains(t, applyErr.Error(), "rollout member eu/testapp-002")
		})
	}
}

// A deployment with one target is shown under its deployment name, "us", but
// a target selector names a target, so the refusal hands back "us-main", the
// selector the server resolves to us/us-main. The display name would be
// refused as naming no rollout member.
func TestRolloutApplyRefusals_NameEachTargetByASelectorTheServerAccepts(t *testing.T) {
	drop := storage.TableChange{Namespace: "testapp", Table: "legacy_orders", Operation: "drop", DDL: "DROP TABLE `legacy_orders`", IsUnsafe: true}
	alter := storage.TableChange{Namespace: "testapp", Table: "users", Operation: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"}
	reviewed := primaryPlanRow("testapp-001")
	reviewed.Environment = "production"
	reviewed.Namespaces = map[string]*storage.NamespacePlanData{"testapp": {Tables: []storage.TableChange{alter}}}
	second := memberPlanRow("plan-second", "testapp-002", "plan-primary")
	second.Namespaces = reviewed.Namespaces
	usMain := memberPlanRow("plan-us-main", "us-main", "plan-primary")
	usMain.Deployment = "us"
	usMain.Namespaces = map[string]*storage.NamespacePlanData{"testapp": {Tables: []storage.TableChange{alter, drop}}}
	svc := memberResolutionService(t, EnvironmentConfig{
		Deployments: map[string]DeploymentTarget{
			"eu": {Targets: targetNames("testapp-001", "testapp-002")},
			"us": {Targets: targetNames("us-main")},
		},
		DeploymentOrder: []string{"eu", "us"},
	}, &listingPlanStore{mockPlanLookupStore: mockPlanLookupStore{plan: reviewed}, plans: []*storage.Plan{second, usMain}})
	targets := targetsFor(t, svc)

	refused, err := svc.rolloutApplyRefusals(t.Context(), "production", "plan-primary", targets)
	require.NoError(t, err)
	require.Len(t, refused, 1)
	assert.Equal(t, "us", refused[0].Member, "the target is shown the way the plan groups name it")
	assert.Equal(t, "us-main", refused[0].Target)
	assert.True(t, refused[0].AllowUnsafe)

	member, err := selectRolloutMember("testapp", "production", targets, refused[0].Target)
	require.NoError(t, err, "the selector handed back names the target")
	assert.Equal(t, "us/us-main", member.MemberID())
	_, err = selectRolloutMember("testapp", "production", targets, refused[0].Member)
	require.Error(t, err, "the display name of a single-target deployment is not a selector")
}

// A schemabot CLI that predates rollout plans reads only the primary's plan
// from a response. It would print "No changes" for a rollout whose primary is
// converged while another target still needs the change, and ask to apply
// the primary's DDL while every target's runs. So a rollout-wide plan of an
// environment with several targets is refused for a caller that does not say
// it renders the rollout, before anything is planned. The narrowed plan of
// one target, and the plan of a single-target environment, are one plan and
// pass, as does a caller that renders the rollout.
func TestHandlePlan_RefusesARolloutWidePlanFromACallerThatDoesNotRenderTheRollout(t *testing.T) {
	for _, tc := range []struct {
		name           string
		config         *ServerConfig
		database       string
		target         string
		rendersRollout bool
		wantRefused    bool
	}{
		{name: "rollout-wide plan of three targets", config: narrowingServerConfig(), database: "payments", wantRefused: true},
		{name: "rollout-wide plan of three targets from a caller that renders the rollout", config: narrowingServerConfig(), database: "payments", rendersRollout: true},
		{name: "plan narrowed to one of three targets", config: narrowingServerConfig(), database: "payments", target: "payments-002"},
		{name: "plan of a single-target environment", config: singleTargetServerConfig(), database: "orders"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &memberDiffClient{mockTernClient: &mockTernClient{isRemote: true, planResp: &ternv1.PlanResponse{PlanId: "plan-converged", Engine: ternv1.Engine_ENGINE_SPIRIT}}}
			svc := New(&mockStorageWithPlanLookup{plans: &recordingPlanStore{}}, tc.config, map[string]tern.Client{
				DefaultDeployment + "/production": client,
			}, slog.New(slog.NewTextHandler(io.Discard, nil)))
			body, err := json.Marshal(apitypes.PlanRequest{
				Database: tc.database, Environment: "production", Type: storage.DatabaseTypeMySQL, Target: tc.target,
				SchemaFiles:    map[string]*apitypes.SchemaFiles{tc.database: {Files: map[string]string{"users.sql": "CREATE TABLE users (id bigint primary key)"}}},
				RendersRollout: tc.rendersRollout,
			})
			require.NoError(t, err)

			w := serveNarrowing(t, svc, "/api/plan", string(body))

			if !tc.wantRefused {
				assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
				return
			}
			requireInvalidRequest(t, w, "payments/production has 3 rollout members, and this client does not show the plan of each one; upgrade the schemabot CLI to plan or apply a multi-target environment")
			assert.Nil(t, client.planReq, "nothing is planned for a refused request")
			assert.Empty(t, client.diffed, "no member is planned for a refused request")
		})
	}
}

// The rollout rendering check counts the environment's rollout members, so a
// rollout-wide plan from a caller that does not render the rollout is refused
// by the check itself when those members cannot be resolved, rather than let
// through to whatever checks come after it.
func TestRefusePlanRolloutUnrenderedByCaller_RefusesAnEnvironmentWhoseMembersDoNotResolve(t *testing.T) {
	svc := New(&mockStorageWithPlanLookup{plans: &recordingPlanStore{}}, narrowingServerConfig(), map[string]tern.Client{},
		slog.New(slog.NewTextHandler(io.Discard, nil)))

	err := svc.refusePlanRolloutUnrenderedByCaller(PlanRequest{Database: "payments", Environment: "sandbox"})
	require.Error(t, err)
	envErr, ok := errors.AsType[*EnvironmentNotConfiguredError](err)
	require.True(t, ok, "the resolution error is returned, not swallowed: %v", err)
	assert.Equal(t, "sandbox", envErr.Environment)
	assert.Contains(t, err.Error(), "resolve rollout members of payments/sandbox")

	require.NoError(t, svc.refusePlanRolloutUnrenderedByCaller(PlanRequest{Database: "payments", Environment: "sandbox", RendersRollout: true}),
		"a caller that renders the rollout is not checked")
}

// The apply side of the same rule: POST /api/apply of a reviewed plan across
// two targets is refused, before anything is stored, for a caller that does
// not say it rendered the rollout, and created for one that does. An apply
// narrowed to one target applies one plan and passes either way.
func TestHandleApply_RefusesARolloutWideApplyFromACallerThatDoesNotRenderTheRollout(t *testing.T) {
	alter := storage.TableChange{Namespace: "testapp", Table: "users", Operation: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"}
	for _, tc := range []struct {
		name        string
		body        string
		target      string
		wantRefused bool
	}{
		{name: "rollout-wide apply", body: `{"plan_id":"plan-primary","environment":"production"}`, wantRefused: true},
		{name: "rollout-wide apply from a caller that renders the rollout", body: `{"plan_id":"plan-primary","environment":"production","renders_rollout":true}`},
		{name: "apply narrowed to the reviewed target", body: `{"plan_id":"plan-primary","environment":"production","target":"testapp-001"}`, target: "testapp-001"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reviewed := primaryPlanRow("testapp-001")
			reviewed.Environment = "production"
			reviewed.Namespaces = map[string]*storage.NamespacePlanData{"testapp": {Tables: []storage.TableChange{alter}}}
			if tc.target != "" {
				reviewed.NarrowedTo = "eu/" + tc.target
			}
			member := memberPlanWithChange(alter)
			svc := multiTargetApplyService(t, &listingPlanStore{mockPlanLookupStore: mockPlanLookupStore{plan: reviewed}, plans: []*storage.Plan{member}})
			svc.ternClients["eu/production"] = &mockTernClient{isRemote: true}

			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/apply", strings.NewReader(tc.body))
			rec := httptest.NewRecorder()
			svc.handleApply(rec, req)

			applies, ok := svc.storage.Applies().(*capturingApplyStore)
			require.True(t, ok)
			if !tc.wantRefused {
				assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				assert.NotNil(t, applies.apply, "the apply is stored")
				return
			}
			requireInvalidRequest(t, rec, "testapp/production has 2 rollout members, and this client does not show the plan of each one; upgrade the schemabot CLI to plan or apply a multi-target environment")
			assert.Nil(t, applies.apply, "nothing is stored for a refused apply")
		})
	}
}
