package github

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEffectiveTarget(t *testing.T) {
	tests := []struct {
		name   string
		info   PullRequestInfo
		target string
		err    string
	}{
		{name: "custom default", info: PullRequestInfo{BaseRef: "trunk", DefaultBranch: "trunk"}, target: "trunk"},
		{name: "ordinary upper member", info: PullRequestInfo{BaseRef: "parent", DefaultBranch: "main"}, target: "parent"},
		{name: "native upper member", info: PullRequestInfo{BaseRef: "parent", DefaultBranch: "main", NativeStack: true, StackBaseRef: "main"}, target: "main"},
		{name: "native release stack", info: PullRequestInfo{BaseRef: "parent", DefaultBranch: "main", NativeStack: true, StackBaseRef: "release"}, target: "release"},
		{name: "missing base", info: PullRequestInfo{DefaultBranch: "main"}, err: "no base branch"},
		{name: "missing default", info: PullRequestInfo{BaseRef: "feature"}, err: "no repository default branch"},
		{name: "incomplete native stack", info: PullRequestInfo{BaseRef: "parent", DefaultBranch: "main", NativeStack: true}, err: "no ultimate base branch"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target, err := tt.info.EffectiveTarget()
			if tt.err != "" {
				require.ErrorContains(t, err, tt.err)
				assert.Empty(t, target)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.target, target)
		})
	}
}

func TestFetchPullRequestNativeStack(t *testing.T) {
	for _, tt := range []struct {
		name   string
		stack  any
		native bool
		target string
		err    string
	}{
		{name: "ordinary", stack: nil, target: "parent"},
		{name: "native", stack: map[string]any{"base": map[string]string{"ref": "trunk"}}, native: true, target: "trunk"},
		{name: "incomplete", stack: map[string]any{}, native: true, err: "no ultimate base branch"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client, mux := setupRateLimitedTestGitHubServer(t)
			calls := 0
			mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
				calls++
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
					"head":  map[string]string{"sha": "head", "ref": "topic"},
					"base":  map[string]any{"ref": "parent", "repo": map[string]string{"default_branch": "trunk"}},
					"state": "open", "stack": tt.stack,
				}))
			})
			ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
			ctx := WithPRInfoCache(t.Context())
			info, err := ic.FetchPullRequest(ctx, "octocat/hello-world", 1)
			require.NoError(t, err)
			assert.Equal(t, tt.native, info.NativeStack)
			assert.Equal(t, "head", info.HeadSHA)
			target, err := info.EffectiveTarget()
			if tt.err != "" {
				require.ErrorContains(t, err, tt.err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.target, target)
			}
			info.StackBaseRef = "mutated"
			cached, err := ic.FetchPullRequest(ctx, "octocat/hello-world", 1)
			require.NoError(t, err)
			assert.NotEqual(t, "mutated", cached.StackBaseRef)
			assert.Equal(t, 1, calls, "native stack metadata shares the PR read and its request cache")
		})
	}
}

// Non-default targets are eligible only after both rulesets and classic
// protection are read successfully. Default targets need no protection reads.
func TestEvaluatePRTarget(t *testing.T) {
	for _, tt := range []struct {
		name        string
		base        string
		stack       string
		rules       string
		classic     string
		rulesStatus int
		eligible    bool
		wantErr     string
	}{
		{name: "custom default", base: "trunk", eligible: true},
		{name: "ordinary feature", base: "feature/parent", rules: `[]`, classic: `null`},
		{name: "ruleset release", base: "release/1", rules: `[{"type":"required_status_checks","parameters":{"required_status_checks":[{"context":"SchemaBot (production)","integration_id":123}]}}]`, classic: `null`, eligible: true},
		{name: "classic release", base: "release/1", rules: `[]`, classic: `{"requiresStatusChecks":true,"requiredStatusCheckContexts":["SchemaBot (staging)"]}`, eligible: true},
		{name: "unrelated protected feature", base: "feature", rules: `[{"type":"required_status_checks","parameters":{"required_status_checks":[{"context":"CI"}]}}]`, classic: `{"requiresStatusChecks":true,"requiredStatusCheckContexts":["Owner review"]}`},
		{name: "native default stack", base: "parent", stack: "trunk", eligible: true},
		{name: "native excluded stack", base: "trunk", stack: "feature", rules: `[]`, classic: `null`},
		{name: "incomplete rule", base: "feature", rules: `[{"type":"required_status_checks","parameters":{}}]`, classic: `null`, wantErr: "omitted required check contexts"},
		{name: "empty required check", base: "feature", rules: `[{"type":"required_status_checks","parameters":{"required_status_checks":[null]}}]`, classic: `null`, wantErr: "empty required check"},
		{name: "missing permission", base: "feature", rulesStatus: http.StatusForbidden, wantErr: "read active branch rules"},
		{name: "rules lookup missing", base: "feature", rulesStatus: http.StatusNotFound, wantErr: "read active branch rules"},
		{name: "classic permission failure", base: "feature", rules: `[]`, classic: `null`, wantErr: "protection denied"},
		{name: "prefix is not a match", base: "feature", rules: `[{"type":"required_status_checks","parameters":{"required_status_checks":[{"context":"SchemaBot impersonator"}]}}]`, classic: `null`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client, mux := setupRateLimitedTestGitHubServer(t)
			var rulesCalls, classicCalls atomic.Int32
			target := tt.base
			if tt.stack != "" {
				target = tt.stack
			}
			mux.HandleFunc("GET /repos/octocat/hello-world/rules/branches/", func(w http.ResponseWriter, r *http.Request) {
				rulesCalls.Add(1)
				assert.Equal(t, "/repos/octocat/hello-world/rules/branches/"+target, r.URL.Path)
				if tt.rulesStatus != 0 {
					w.WriteHeader(tt.rulesStatus)
					return
				}
				_, err := io.WriteString(w, tt.rules)
				require.NoError(t, err)
			})
			mux.HandleFunc("POST /graphql", func(w http.ResponseWriter, r *http.Request) {
				classicCalls.Add(1)
				var request struct {
					Variables map[string]string `json:"variables"`
				}
				require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
				assert.Equal(t, "refs/heads/"+target, request.Variables["ref"])
				if tt.wantErr == "protection denied" {
					_, err := io.WriteString(w, `{"errors":[{"message":"protection denied"}]}`)
					require.NoError(t, err)
					return
				}
				_, err := fmt.Fprintf(w, `{"data":{"repository":{"ref":{"name":%q,"branchProtectionRule":%s}}}}`, target, tt.classic)
				require.NoError(t, err)
			})
			ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
			info := &PullRequestInfo{BaseRef: tt.base, DefaultBranch: "trunk", NativeStack: tt.stack != "", StackBaseRef: tt.stack}
			ctx := WithPRInfoCache(t.Context())
			decision, err := ic.EvaluatePRTarget(ctx, "octocat/hello-world", info, []string{"SchemaBot"})
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				assert.False(t, decision.Eligible)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.eligible, decision.Eligible)
			assert.Equal(t, target, decision.Target)
			_, err = ic.EvaluatePRTarget(ctx, "octocat/hello-world", info, []string{"SchemaBot"})
			require.NoError(t, err)
			if target == "trunk" {
				assert.Equal(t, int32(0), rulesCalls.Load())
				assert.Equal(t, int32(0), classicCalls.Load())
			} else {
				assert.Equal(t, int32(1), rulesCalls.Load(), "one ruleset lookup per target within a delivery")
				assert.Equal(t, int32(1), classicCalls.Load(), "one classic lookup per target within a delivery")
			}
		})
	}
}
