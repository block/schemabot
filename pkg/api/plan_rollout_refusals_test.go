package api

import (
	"encoding/json"
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
