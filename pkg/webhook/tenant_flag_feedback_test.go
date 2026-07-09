package webhook

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
)

// tenantFeedbackLeaderConfig returns a server config where this deployment is
// the aggregate leader for the test repo, serves tenant "tenant-a" itself, and
// expects participants "tenant-b" and "tenant-c" on the repo.
func tenantFeedbackLeaderConfig() *api.ServerConfig {
	return &api.ServerConfig{
		Tenant: "tenant-a",
		Repos: map[string]api.RepoConfig{
			"octocat/hello-world": {Aggregate: &api.AggregateConfig{
				Role: api.AggregateRoleLeader,
				ExpectedTenants: []api.ExpectedTenant{
					{Tenant: "tenant-b", Paths: []string{"tenant-b/schema"}, CheckName: "SchemaBot Tenant B"},
					{Tenant: "tenant-c", Paths: []string{"tenant-c/schema"}, CheckName: "SchemaBot Tenant C"},
				},
			}},
		},
	}
}

// tenantFeedbackParticipantConfig returns a server config where this
// deployment is an aggregate participant for the test repo, serving tenant
// "tenant-b".
func tenantFeedbackParticipantConfig() *api.ServerConfig {
	return &api.ServerConfig{
		Tenant: "tenant-b",
		Repos: map[string]api.RepoConfig{
			"octocat/hello-world": {Aggregate: &api.AggregateConfig{Role: api.AggregateRoleParticipant}},
		},
	}
}

// serveIssueCommentWebhook delivers a PR comment through the full webhook path
// and returns the recorded HTTP response.
func serveIssueCommentWebhook(t *testing.T, h *Handler, comment string) *httptest.ResponseRecorder {
	t.Helper()
	req := buildWebhookRequest(t, webhookPayloadOpts{comment: comment, isPR: true}, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	return rr
}

// A --tenant/-t flag SchemaBot cannot read a tenant name from routes to no
// deployment, so on a shared repo every deployment would otherwise stay
// silent and the user would get zero feedback. The aggregate leader posts the
// one visible answer showing the correct flag syntax.
func TestTenantFlagFeedbackLeaderAnswersInvalidTenantFlag(t *testing.T) {
	for _, comment := range []string{
		"schemabot plan -e staging --tenant",
		"schemabot plan -e staging -typo",
	} {
		t.Run(comment, func(t *testing.T) {
			h, _, comments := newFanOutSkipHandler(t, tenantFeedbackLeaderConfig())

			rr := serveIssueCommentWebhook(t, h, comment)

			assert.Contains(t, rr.Body.String(), "invalid tenant flag")
			body := requireComment(t, comments, "invalid-tenant-flag comment")
			assert.Contains(t, body, "Invalid Tenant Flag")
			assert.Contains(t, body, "`--tenant <name>`")
			assert.Contains(t, body, "`-t <name>`")
		})
	}
}

// A command targeting a tenant no deployment on the repo serves would leave
// every deployment silent, so the aggregate leader — the one deployment that
// knows the repo's full expected-tenant set — answers with the tenants that
// are actually routable: the expected participants plus the leader's own
// tenant.
func TestTenantFlagFeedbackLeaderAnswersUnknownTenant(t *testing.T) {
	h, _, comments := newFanOutSkipHandler(t, tenantFeedbackLeaderConfig())

	rr := serveIssueCommentWebhook(t, h, "schemabot plan -e staging --tenant gamma")

	assert.Contains(t, rr.Body.String(), "unknown tenant")
	body := requireComment(t, comments, "unknown-tenant comment")
	assert.Contains(t, body, "Unknown Tenant")
	assert.Contains(t, body, "`gamma`")
	assert.Contains(t, body, "- `tenant-b`")
	assert.Contains(t, body, "- `tenant-c`")
	assert.Contains(t, body, "- `tenant-a`")
}

// A tenant the leader knows but does not serve is owned by a participant
// deployment, which responds from its own delivery of the same webhook. The
// leader stays silent so the PR gets exactly one answer.
func TestTenantFlagFeedbackLeaderSilentForParticipantOwnedTenant(t *testing.T) {
	h, _, comments := newFanOutSkipHandler(t, tenantFeedbackLeaderConfig())

	rr := serveIssueCommentWebhook(t, h, "schemabot plan -e staging --tenant tenant-b")

	assert.Contains(t, rr.Body.String(), "tenant handled by another instance")
	select {
	case body := <-comments:
		t.Fatalf("leader must not post for a participant-owned tenant, got comment: %s", body)
	default:
	}
}

// Only the aggregate leader answers tenant-routing errors: a participant and a
// deployment on a repo without any aggregate config both keep the standard
// silent behavior for a broken tenant flag and for a tenant they do not serve,
// so a shared repo never collects duplicate answers and single-instance
// deployments are unaffected.
func TestTenantFlagFeedbackNonLeadersStaySilent(t *testing.T) {
	configs := map[string]*api.ServerConfig{
		"participant":         tenantFeedbackParticipantConfig(),
		"no aggregate config": nonAggregateConfig(),
	}
	cases := []struct {
		name     string
		comment  string
		response string
	}{
		{name: "invalid tenant flag", comment: "schemabot plan -e staging --tenant", response: "invalid tenant flag"},
		{name: "unknown tenant", comment: "schemabot plan -e staging --tenant gamma", response: "tenant handled by another instance"},
	}
	for role, cfg := range configs {
		for _, tc := range cases {
			t.Run(role+"/"+tc.name, func(t *testing.T) {
				h, _, comments := newFanOutSkipHandler(t, cfg)

				rr := serveIssueCommentWebhook(t, h, tc.comment)

				assert.Contains(t, rr.Body.String(), tc.response)
				select {
				case body := <-comments:
					t.Fatalf("non-leader must stay silent, got comment: %s", body)
				default:
				}
			})
		}
	}
}
