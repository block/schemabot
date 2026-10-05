package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	ghclient "github.com/block/schemabot/pkg/github"
)

// A plan narrowed to payments-002 says nothing about payments-001 or
// payments-003, so even a clean, change-free narrowed plan must never be
// recorded as the database's stored check state: that would let one member's
// result stand for the whole rollout and pass the merge gate while other
// targets still need the change. The write is refused before any storage or
// GitHub call.
func TestUpsertPlanCheckRecordRefusesANarrowedPlan(t *testing.T) {
	checks := &driftBlockedCheckStore{}
	service := api.New(&driftBlockedStorage{checks: checks}, &api.ServerConfig{
		Repos: map[string]api.RepoConfig{},
	}, nil, testLogger())
	h := &Handler{service: service, logger: testLogger()}

	schema := &ghclient.SchemaRequestResult{Database: "payments", Type: "mysql", HeadSHA: "abc123"}
	planResp := &apitypes.PlanResponse{PlanID: "plan-narrowed", NarrowedTo: "prod/payments-002"}

	headSHA, check, err := h.upsertPlanCheckRecord(t.Context(), nil, "octocat/hello-world", 1, schema, planResp, "production",
		reviewDriftOutcome{state: driftClean})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "narrowed to rollout member prod/payments-002")
	assert.Empty(t, headSHA)
	assert.Nil(t, check)
	assert.Zero(t, checks.upsertCalls, "no stored check state is written for a narrowed plan")
}
