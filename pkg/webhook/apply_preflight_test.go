package webhook

import (
	"encoding/json"
	"net/http"
	"testing"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
)

// preflightSchemaResult is a discovery result scoped to the first environment
// in its own promotion order, so the prior-environment probe has nothing ahead
// of it and the checklist is about the gates the test is exercising.
func preflightSchemaResult() *ghclient.SchemaRequestResult {
	return &ghclient.SchemaRequestResult{
		Database:     "orders",
		Type:         "mysql",
		SchemaPath:   "schema/testdb",
		HeadSHA:      "abc123",
		Environments: []string{"staging"},
	}
}

// registerPreflightCheckStatuses serves the PR's commit status and check runs
// for the head the schema result was discovered at.
func registerPreflightCheckStatuses(mux *http.ServeMux, nodes []checkStatusNode) {
	mux.HandleFunc("GET /repos/octocat/hello-world/commits/abc123/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		writeCommitStatusResponse(w, nodes)
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/commits/abc123/check-runs", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		writeCheckRunsResponse(w, nodes)
	})
}

func registerPreflightComments(t *testing.T, mux *http.ServeMux) chan string {
	t.Helper()
	comments := make(chan string, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, comments))
	return comments
}

// An apply runs a ladder of gates and the first one that blocks answers the
// command, so an operator who clears it can land straight on the next. The
// rejection comment therefore also reports the gates behind the one that
// blocked: everything still in the way is named once, and a retry after
// clearing them all gets through. The report is read-only and decides
// nothing — the blocking gate is still the command's answer.
func TestApplyPreflightChecklistOnGateRejection(t *testing.T) {
	t.Run("a review block also names the failing checks behind it", func(t *testing.T) {
		h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
			db := cfg.Databases["orders"]
			db.OperatorUsers = []string{"bob"}
			cfg.Databases["orders"] = db
		}))
		registerPREndpoint(mux, "alice")
		registerReviewsEndpoint(mux, []*gh.PullRequestReview{})
		registerPreflightCheckStatuses(mux, []checkStatusNode{
			{Typename: "CheckRun", Name: "build", Status: "completed", Conclusion: "failure", AppSlug: "github-actions"},
		})
		comments := registerPreflightComments(t, mux)

		client, err := h.clientForRepo("octocat/hello-world", 12345)
		require.NoError(t, err)

		schemaResult := preflightSchemaResult()
		preflight := h.newApplyPreflight(client, "octocat/hello-world", 1, schemaResult, "staging")
		blocked, err := h.enforceReviewGate(t.Context(), client, "octocat/hello-world", 1, 12345, schemaResult, "staging", "alice", "apply", false, preflight)
		require.NoError(t, err)
		assert.True(t, blocked)

		body := requireComment(t, comments, "review-required comment")
		assert.Contains(t, body, "Review Required", "the blocking gate still owns the comment")
		assert.Contains(t, body, "| PR checks | ⚠️ 1 check not passing |")
		assert.Contains(t, body, "| Prior environments | ✅ Ready |")
		assert.Contains(t, body, "| Database lock | ✅ Ready |")
	})

	t.Run("a review block with everything else clear says the retry will get through", func(t *testing.T) {
		h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
			db := cfg.Databases["orders"]
			db.OperatorUsers = []string{"bob"}
			cfg.Databases["orders"] = db
		}))
		registerPREndpoint(mux, "alice")
		registerReviewsEndpoint(mux, []*gh.PullRequestReview{})
		registerPreflightCheckStatuses(mux, []checkStatusNode{
			{Typename: "CheckRun", Name: "build", Status: "completed", Conclusion: "success", AppSlug: "github-actions"},
		})
		comments := registerPreflightComments(t, mux)

		client, err := h.clientForRepo("octocat/hello-world", 12345)
		require.NoError(t, err)

		schemaResult := preflightSchemaResult()
		preflight := h.newApplyPreflight(client, "octocat/hello-world", 1, schemaResult, "staging")
		blocked, err := h.enforceReviewGate(t.Context(), client, "octocat/hello-world", 1, 12345, schemaResult, "staging", "alice", "apply", false, preflight)
		require.NoError(t, err)
		assert.True(t, blocked)

		body := requireComment(t, comments, "review-required comment")
		assert.Contains(t, body, "Nothing else blocks it.")
	})

	t.Run("a gate whose inputs cannot be read is reported unknown, never ready", func(t *testing.T) {
		h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
			db := cfg.Databases["orders"]
			db.OperatorUsers = []string{"bob"}
			cfg.Databases["orders"] = db
		}))
		registerPREndpoint(mux, "alice")
		registerReviewsEndpoint(mux, []*gh.PullRequestReview{})
		denied := func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]any{"message": "Resource not accessible by integration"})
		}
		mux.HandleFunc("GET /repos/octocat/hello-world/commits/abc123/status", denied)
		mux.HandleFunc("GET /repos/octocat/hello-world/commits/abc123/check-runs", denied)
		comments := registerPreflightComments(t, mux)

		client, err := h.clientForRepo("octocat/hello-world", 12345)
		require.NoError(t, err)

		schemaResult := preflightSchemaResult()
		preflight := h.newApplyPreflight(client, "octocat/hello-world", 1, schemaResult, "staging")
		blocked, err := h.enforceReviewGate(t.Context(), client, "octocat/hello-world", 1, 12345, schemaResult, "staging", "alice", "apply", false, preflight)
		require.NoError(t, err)
		assert.True(t, blocked)

		body := requireComment(t, comments, "review-required comment")
		assert.Contains(t, body, "| PR checks | ℹ️ Check statuses could not be read |")
		assert.NotContains(t, body, "Resource not accessible",
			"raw GitHub error text must never render in PR markdown")
	})

	t.Run("no prober leaves the rejection comment exactly as it was", func(t *testing.T) {
		h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
			db := cfg.Databases["orders"]
			db.OperatorUsers = []string{"bob"}
			cfg.Databases["orders"] = db
		}))
		registerPREndpoint(mux, "alice")
		registerReviewsEndpoint(mux, []*gh.PullRequestReview{})
		comments := registerPreflightComments(t, mux)

		client, err := h.clientForRepo("octocat/hello-world", 12345)
		require.NoError(t, err)

		blocked, err := h.enforceReviewGate(t.Context(), client, "octocat/hello-world", 1, 12345, preflightSchemaResult(), "staging", "alice", "apply", false, nil)
		require.NoError(t, err)
		assert.True(t, blocked)

		body := requireComment(t, comments, "review-required comment")
		assert.NotContains(t, body, "Remaining before this apply can run")
	})
}

// The checklist's job on a prior environment is to tell an operator whether to
// wait or to act, so the reasons the gate itself distinguishes must survive
// into the row's detail: a change still copying is not a failure, and a plan
// nobody has applied is neither.
func TestPriorEnvironmentBlockedDetail(t *testing.T) {
	tests := []struct {
		name       string
		status     string
		conclusion string
		want       string
	}{
		{
			name:   "an in-progress check is something to wait for",
			status: checkStatusInProgress,
			want:   "staging is still running",
		},
		{
			name:   "a queued check has not started and is still something to wait for",
			status: checkStatusQueued,
			want:   "staging is still running",
		},
		{
			name:       "a failed check is something to fix",
			status:     checkStatusCompleted,
			conclusion: checkConclusionFailure,
			want:       "staging failed",
		},
		{
			name:       "anything else is a change nobody has applied yet",
			status:     checkStatusCompleted,
			conclusion: checkConclusionActionRequired,
			want:       "staging has pending changes",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, priorEnvironmentBlockedDetail("staging", tt.status, tt.conclusion))
		})
	}
}
