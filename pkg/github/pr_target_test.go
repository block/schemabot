package github

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
		classicRef  string
		rulesStatus int
		eligible    bool
		unreadable  bool
		forbidden   string
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
		{name: "classic protection unreadable without Administration", base: "feature", rules: `[]`, forbidden: `[{"type":"FORBIDDEN","path":["repository","ref","branchProtectionRule"],"message":"Resource not accessible by integration"}]`, eligible: true, unreadable: true},
		{name: "ruleset match wins over unreadable classic protection", base: "release/1", rules: `[{"type":"required_status_checks","parameters":{"required_status_checks":[{"context":"SchemaBot (production)"}]}}]`, forbidden: `[{"type":"FORBIDDEN","message":"Resource not accessible by integration"}]`, eligible: true},
		{name: "forbidden mixed with another error stays an error", base: "feature", rules: `[]`, forbidden: `[{"type":"FORBIDDEN","message":"Resource not accessible by integration"},{"type":"INTERNAL","message":"something broke"}]`, wantErr: "Resource not accessible by integration"},
		{name: "prefix is not a match", base: "feature", rules: `[{"type":"required_status_checks","parameters":{"required_status_checks":[{"context":"SchemaBot impersonator"}]}}]`, classic: `null`},
		{name: "missing closing parenthesis", base: "feature", rules: `[{"type":"required_status_checks","parameters":{"required_status_checks":[{"context":"SchemaBot (production"}]}}]`, classic: `null`},
		{name: "classic lookup returns a different branch", base: "feature", rules: `[]`, classicRef: "release", classic: `{"requiresStatusChecks":true,"requiredStatusCheckContexts":["SchemaBot (staging)"]}`, wantErr: `classic branch protection query returned branch "release", expected "feature"`},
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
				if tt.forbidden != "" {
					_, err := fmt.Fprintf(w, `{"data":{"repository":{"ref":{"name":%q,"branchProtectionRule":null}}},"errors":%s}`, target, tt.forbidden)
					require.NoError(t, err)
					return
				}
				if tt.wantErr == "protection denied" {
					_, err := io.WriteString(w, `{"errors":[{"message":"protection denied"}]}`)
					require.NoError(t, err)
					return
				}
				ref := tt.classicRef
				if ref == "" {
					ref = target
				}
				_, err := fmt.Fprintf(w, `{"data":{"repository":{"ref":{"name":%q,"branchProtectionRule":%s}}}}`, ref, tt.classic)
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
			assert.Equal(t, tt.unreadable, decision.ClassicProtectionUnreadable)
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

type policyWaitContext struct {
	context.Context
	waiting chan struct{}
}

func (c policyWaitContext) Done() <-chan struct{} {
	// The caller consults Done after joining the in-flight read.
	select {
	case c.waiting <- struct{}{}:
	default:
	}
	return c.Context.Done()
}

func receivePolicyTestResult[T any](t *testing.T, ctx context.Context, ready <-chan T) T {
	t.Helper()
	select {
	case result := <-ready:
		return result
	case <-ctx.Done():
		require.NoError(t, ctx.Err(), "wait for branch policy test")
		var zero T
		return zero
	}
}

type policyTestResult struct {
	decision PRTargetEligibility
	err      error
}

func evaluatePolicyInBackground(ctx context.Context, client *InstallationClient, repo, branch, checkBase string) <-chan policyTestResult {
	result := make(chan policyTestResult, 1)
	go func() {
		decision, err := client.EvaluatePRTarget(ctx, repo, &PullRequestInfo{BaseRef: branch, DefaultBranch: "main"}, []string{checkBase})
		result <- policyTestResult{decision: decision, err: err}
	}()
	return result
}

func writePolicyClassicResponse(t *testing.T, w http.ResponseWriter, r *http.Request, protection string) {
	t.Helper()
	var request struct {
		Variables map[string]string `json:"variables"`
	}
	require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
	branch := strings.TrimPrefix(request.Variables["ref"], "refs/heads/")
	_, err := fmt.Fprintf(w, `{"data":{"repository":{"ref":{"name":%q,"branchProtectionRule":%s}}}}`, branch, protection)
	require.NoError(t, err)
}

func TestEvaluatePRTargetSharesOverlappingPolicyReads(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	client, mux := setupRateLimitedTestGitHubServer(t)
	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	now := time.Unix(0, 0)
	ic.branchPolicies.now = func() time.Time { return now }
	var ruleReads, classicReads atomic.Int32
	var protectionChanged atomic.Bool
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	finish := sync.OnceFunc(func() { close(release) })
	defer finish()

	mux.HandleFunc("GET /repos/{owner}/{repo}/rules/branches/release", func(w http.ResponseWriter, r *http.Request) {
		if ruleReads.Add(1) == 1 {
			started <- struct{}{}
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		if !protectionChanged.Load() {
			if r.URL.Query().Get("page") == "1" {
				w.Header().Set("Link", `<?page=2>; rel="next"`)
			} else {
				_, err := io.WriteString(w, `[{"type":"required_status_checks","parameters":{"required_status_checks":[{"context":"Rule plan (staging)"}]}}]`)
				require.NoError(t, err)
				return
			}
		}
		_, err := io.WriteString(w, `[]`)
		require.NoError(t, err)
	})
	mux.HandleFunc("POST /graphql", func(w http.ResponseWriter, r *http.Request) {
		classicReads.Add(1)
		rule := `{"requiresStatusChecks":true,"requiredStatusCheckContexts":["SchemaBot (staging)"]}`
		if protectionChanged.Load() {
			rule = "null"
		}
		writePolicyClassicResponse(t, w, r, rule)
	})

	first := evaluatePolicyInBackground(WithPRInfoCache(ctx), ic, "octocat/hello-world", "release", "SchemaBot")
	receivePolicyTestResult(t, ctx, started)
	waiter := policyWaitContext{Context: WithPRInfoCache(ctx), waiting: make(chan struct{}, 1)}
	second := evaluatePolicyInBackground(waiter, ic, "OctoCat/Hello-World", "release", "Other check")
	receivePolicyTestResult(t, ctx, waiter.waiting)
	finish()

	firstResult := receivePolicyTestResult(t, ctx, first)
	require.NoError(t, firstResult.err)
	assert.True(t, firstResult.decision.Eligible, "classic protection contributes to the shared policy")
	secondResult := receivePolicyTestResult(t, ctx, second)
	require.NoError(t, secondResult.err)
	assert.False(t, secondResult.decision.Eligible, "each caller matches its own check names")
	assert.Equal(t, int32(2), ruleReads.Load(), "one shared paginated rules read")
	assert.Equal(t, int32(1), classicReads.Load(), "one shared classic protection read")

	protectionChanged.Store(true)
	info := &PullRequestInfo{BaseRef: "release", DefaultBranch: "main"}
	decision, err := ic.EvaluatePRTarget(waiter, "octocat/hello-world", info, []string{"Rule plan"})
	require.NoError(t, err)
	assert.True(t, decision.Eligible, "the delivery retains the complete shared list, including the second rules page")
	assert.Equal(t, int32(2), ruleReads.Load())
	assert.Equal(t, int32(1), classicReads.Load())

	now = now.Add(branchPolicyTTL - time.Second)
	decision, err = ic.EvaluatePRTarget(WithPRInfoCache(ctx), "OctoCat/Hello-World", info, []string{"SchemaBot", "Rule plan"})
	require.NoError(t, err)
	assert.True(t, decision.Eligible, "another delivery reuses both policy responses for the same canonical repository")
	assert.Equal(t, int32(2), ruleReads.Load())
	assert.Equal(t, int32(1), classicReads.Load())

	now = now.Add(time.Second)
	decision, err = ic.EvaluatePRTarget(waiter, "octocat/hello-world", info, []string{"SchemaBot", "Rule plan"})
	require.NoError(t, err)
	assert.False(t, decision.Eligible, "neither prior hits nor the delivery context extend the original expiry")
	assert.Equal(t, int32(3), ruleReads.Load())
	assert.Equal(t, int32(2), classicReads.Load())
}

func TestEvaluatePRTargetPolicyReadSurvivesCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	client, mux := setupRateLimitedTestGitHubServer(t)
	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	var reads atomic.Int32
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	finish := sync.OnceFunc(func() { close(release) })
	defer finish()

	mux.HandleFunc("GET /repos/octocat/hello-world/rules/branches/release", func(w http.ResponseWriter, r *http.Request) {
		if reads.Add(1) == 1 {
			started <- struct{}{}
		}
		select {
		case <-release:
			_, err := io.WriteString(w, `[]`)
			require.NoError(t, err)
		case <-r.Context().Done():
		}
	})
	mux.HandleFunc("POST /graphql", func(w http.ResponseWriter, r *http.Request) {
		writePolicyClassicResponse(t, w, r, `{"requiresStatusChecks":true,"requiredStatusCheckContexts":["SchemaBot"]}`)
	})

	leaderCtx, cancelLeader := context.WithCancel(WithPRInfoCache(ctx))
	defer cancelLeader()
	leader := evaluatePolicyInBackground(leaderCtx, ic, "octocat/hello-world", "release", "SchemaBot")
	receivePolicyTestResult(t, ctx, started)

	waiterCtx, cancelWaiter := context.WithCancel(WithPRInfoCache(ctx))
	defer cancelWaiter()
	waiter := policyWaitContext{Context: waiterCtx, waiting: make(chan struct{}, 1)}
	waiting := evaluatePolicyInBackground(waiter, ic, "octocat/hello-world", "release", "SchemaBot")
	receivePolicyTestResult(t, ctx, waiter.waiting)
	survivor := policyWaitContext{Context: WithPRInfoCache(ctx), waiting: make(chan struct{}, 1)}
	surviving := evaluatePolicyInBackground(survivor, ic, "octocat/hello-world", "release", "SchemaBot")
	receivePolicyTestResult(t, ctx, survivor.waiting)

	cancelLeader()
	cancelWaiter()
	leaderResult := receivePolicyTestResult(t, ctx, leader)
	waiterResult := receivePolicyTestResult(t, ctx, waiting)
	assert.ErrorIs(t, leaderResult.err, context.Canceled)
	assert.ErrorIs(t, waiterResult.err, context.Canceled)
	finish()

	result := receivePolicyTestResult(t, ctx, surviving)
	require.NoError(t, result.err)
	assert.True(t, result.decision.Eligible)
	assert.Equal(t, int32(1), reads.Load(), "cancelling either caller leaves one shared read")
}

func TestEvaluatePRTargetSeparatesPolicyReadIdentities(t *testing.T) {
	ctx, cancel := context.WithTimeout(WithPRInfoCache(t.Context()), 5*time.Second)
	defer cancel()
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	finish := sync.OnceFunc(func() { close(release) })
	defer finish()

	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("GET /repos/{owner}/{repo}/rules/branches/{branch}", func(w http.ResponseWriter, r *http.Request) {
		response := `[]`
		if r.PathValue("repo") == "hello-world" && r.PathValue("branch") == "release" {
			started <- struct{}{}
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
			response = `[{"type":"required_status_checks","parameters":{"required_status_checks":[{"context":"SchemaBot"}]}}]`
		}
		_, err := io.WriteString(w, response)
		require.NoError(t, err)
	})
	mux.HandleFunc("POST /graphql", func(w http.ResponseWriter, r *http.Request) {
		writePolicyClassicResponse(t, w, r, "null")
	})

	otherClient, otherMux := setupRateLimitedTestGitHubServer(t)
	otherMux.HandleFunc("GET /repos/octocat/hello-world/rules/branches/release", func(w http.ResponseWriter, _ *http.Request) {
		_, err := io.WriteString(w, `[]`)
		require.NoError(t, err)
	})
	otherMux.HandleFunc("POST /graphql", func(w http.ResponseWriter, r *http.Request) {
		writePolicyClassicResponse(t, w, r, "null")
	})

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ic := NewInstallationClient(client, logger)
	otherInstallation := NewInstallationClient(otherClient, logger)
	leader := evaluatePolicyInBackground(ctx, ic, "octocat/hello-world", "release", "SchemaBot")
	receivePolicyTestResult(t, ctx, started)

	for _, tc := range []struct {
		name   string
		client *InstallationClient
		repo   string
		branch string
	}{
		{name: "another repository", client: ic, repo: "octocat/other", branch: "release"},
		{name: "case-sensitive branch", client: ic, repo: "octocat/hello-world", branch: "Release"},
		{name: "another installation client", client: otherInstallation, repo: "octocat/hello-world", branch: "release"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info := &PullRequestInfo{BaseRef: tc.branch, DefaultBranch: "main"}
			decision, err := tc.client.EvaluatePRTarget(ctx, tc.repo, info, []string{"SchemaBot"})
			require.NoError(t, err)
			assert.False(t, decision.Eligible, "a separate policy lookup finishes independently")
		})
	}
	finish()
	result := receivePolicyTestResult(t, ctx, leader)
	require.NoError(t, result.err)
	assert.True(t, result.decision.Eligible)

	info := &PullRequestInfo{BaseRef: "release", DefaultBranch: "main"}
	decision, err := otherInstallation.EvaluatePRTarget(ctx, "octocat/hello-world", info, []string{"SchemaBot"})
	require.NoError(t, err)
	assert.False(t, decision.Eligible, "policy caches stay isolated even within a shared context")
}

func TestEvaluatePRTargetRetriesFailedSharedPolicyRead(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	var rulesReads, classicReads atomic.Int32
	mux.HandleFunc("GET /repos/octocat/hello-world/rules/branches/release", func(w http.ResponseWriter, _ *http.Request) {
		rulesReads.Add(1)
		_, err := io.WriteString(w, `[{"type":"required_status_checks","parameters":{"required_status_checks":[{"context":"SchemaBot"}]}}]`)
		require.NoError(t, err)
	})
	mux.HandleFunc("POST /graphql", func(w http.ResponseWriter, r *http.Request) {
		if classicReads.Add(1) == 1 {
			_, err := io.WriteString(w, `{"errors":[{"message":"protection denied"}]}`)
			require.NoError(t, err)
			return
		}
		writePolicyClassicResponse(t, w, r, "null")
	})

	ctx := WithPRInfoCache(t.Context())
	info := &PullRequestInfo{BaseRef: "release", DefaultBranch: "main"}
	decision, err := ic.EvaluatePRTarget(ctx, "octocat/hello-world", info, []string{"SchemaBot"})
	require.ErrorContains(t, err, "protection denied")
	assert.False(t, decision.Eligible, "partial rules alone do not establish policy")

	decision, err = ic.EvaluatePRTarget(ctx, "octocat/hello-world", info, []string{"SchemaBot"})
	require.NoError(t, err)
	assert.True(t, decision.Eligible, "the next read retries both policy sources")
	assert.Equal(t, int32(2), rulesReads.Load())
	assert.Equal(t, int32(2), classicReads.Load())
}

func TestBranchPolicyCachesEmptyAndRejectsExpiredResultsOnError(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	now := time.Unix(0, 0)
	ic.branchPolicies.now = func() time.Time { return now }
	var rulesReads, classicReads atomic.Int32
	mux.HandleFunc("GET /repos/octocat/hello-world/rules/branches/release", func(w http.ResponseWriter, _ *http.Request) {
		rulesReads.Add(1)
		_, err := io.WriteString(w, "[]")
		require.NoError(t, err)
	})
	mux.HandleFunc("POST /graphql", func(w http.ResponseWriter, r *http.Request) {
		switch classicReads.Add(1) {
		case 1:
			writePolicyClassicResponse(t, w, r, "null")
		case 2, 4, 5:
			writePolicyClassicResponse(t, w, r, `{"requiresStatusChecks":true,"requiredStatusCheckContexts":["SchemaBot"]}`)
		case 3, 6:
			_, err := io.WriteString(w, `{"errors":[{"message":"protection denied"}]}`)
			require.NoError(t, err)
		}
	})

	ctx := WithPRInfoCache(t.Context())
	info := &PullRequestInfo{BaseRef: "release", DefaultBranch: "main"}
	checks := []string{"SchemaBot"}
	decision, err := ic.EvaluatePRTarget(ctx, "octocat/hello-world", info, checks)
	require.NoError(t, err)
	assert.False(t, decision.Eligible)
	decision, err = ic.EvaluatePRTarget(WithPRInfoCache(t.Context()), "octocat/hello-world", info, checks)
	require.NoError(t, err)
	assert.False(t, decision.Eligible)
	assert.Equal(t, int32(1), rulesReads.Load(), "a complete empty policy is a cache hit")

	decision, err = ic.EvaluatePRTargetFresh(ctx, "octocat/hello-world", info, checks)
	require.NoError(t, err)
	assert.True(t, decision.Eligible, "a fresh read bypasses a cached negative policy")
	assert.Equal(t, int32(2), classicReads.Load())

	now = now.Add(branchPolicyTTL)
	decision, err = ic.EvaluatePRTarget(ctx, "octocat/hello-world", info, checks)
	require.ErrorContains(t, err, "protection denied")
	assert.False(t, decision.Eligible)
	decision, err = ic.EvaluatePRTarget(ctx, "octocat/hello-world", info, checks)
	require.NoError(t, err)
	assert.True(t, decision.Eligible)
	assert.Equal(t, int32(4), rulesReads.Load(), "a failed pair is retried")

	now = now.Add(branchPolicyTTL)
	decision, err = ic.EvaluatePRTarget(ctx, "octocat/hello-world", info, checks)
	require.NoError(t, err)
	assert.True(t, decision.Eligible)
	now = now.Add(branchPolicyTTL)
	decision, err = ic.EvaluatePRTarget(ctx, "octocat/hello-world", info, checks)
	require.ErrorContains(t, err, "protection denied")
	assert.False(t, decision.Eligible, "an expired eligible policy cannot substitute for a GitHub error")
	assert.Equal(t, int32(6), rulesReads.Load())
	assert.Equal(t, int32(6), classicReads.Load())
}

func TestFreshPRTargetPolicyDoesNotJoinBackgroundRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	client, mux := setupRateLimitedTestGitHubServer(t)
	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	finish := sync.OnceFunc(func() { close(release) })
	defer finish()
	var reads atomic.Int32
	mux.HandleFunc("GET /repos/octocat/hello-world/rules/branches/release", func(w http.ResponseWriter, r *http.Request) {
		if reads.Add(1) == 1 {
			started <- struct{}{}
			select {
			case <-release:
				_, err := io.WriteString(w, `[{"type":"required_status_checks","parameters":{"required_status_checks":[{"context":"SchemaBot"}]}}]`)
				require.NoError(t, err)
			case <-r.Context().Done():
			}
			return
		}
		_, err := io.WriteString(w, "[]")
		require.NoError(t, err)
	})
	mux.HandleFunc("POST /graphql", func(w http.ResponseWriter, r *http.Request) {
		writePolicyClassicResponse(t, w, r, "null")
	})

	background := evaluatePolicyInBackground(ctx, ic, "octocat/hello-world", "release", "SchemaBot")
	receivePolicyTestResult(t, ctx, started)
	info := &PullRequestInfo{BaseRef: "release", DefaultBranch: "main"}
	decision, err := ic.EvaluatePRTargetFresh(ctx, "octocat/hello-world", info, []string{"SchemaBot"})
	require.NoError(t, err)
	assert.False(t, decision.Eligible, "the live apply check finishes independently of the blocked older read")
	assert.Equal(t, int32(2), reads.Load())
	finish()
	result := receivePolicyTestResult(t, ctx, background)
	require.NoError(t, result.err)
	assert.True(t, result.decision.Eligible)
}

func TestBranchPolicyCacheCopiesNames(t *testing.T) {
	var cache branchPolicyCache
	names := []string{"SchemaBot"}
	cache.put("release", branchPolicy{contexts: names, classicUnreadable: true})
	names[0] = "mutated input"
	cached, ok := cache.get("release")
	require.True(t, ok)
	assert.Equal(t, branchPolicy{contexts: []string{"SchemaBot"}, classicUnreadable: true}, cached)
	cached.contexts[0] = "mutated result"
	cached, ok = cache.get("release")
	require.True(t, ok)
	assert.Equal(t, branchPolicy{contexts: []string{"SchemaBot"}, classicUnreadable: true}, cached)
}

// An App without Administration: read cannot see classic branch protection.
// Apply acceptance then treats a non-default target as enforced, as SchemaBot
// did before target enforcement, and warns that the permission is missing.
func TestFreshPRTargetEnforcesWhenClassicProtectionIsUnreadable(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world/rules/branches/feature", func(w http.ResponseWriter, _ *http.Request) {
		_, err := io.WriteString(w, `[]`)
		require.NoError(t, err)
	})
	mux.HandleFunc("POST /graphql", func(w http.ResponseWriter, _ *http.Request) {
		_, err := io.WriteString(w, `{"data":{"repository":{"ref":{"name":"feature","branchProtectionRule":null}}},"errors":[{"type":"FORBIDDEN","path":["repository","ref","branchProtectionRule"],"message":"Resource not accessible by integration"}]}`)
		require.NoError(t, err)
	})
	var logs bytes.Buffer
	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(&logs, nil)))

	decision, err := ic.EvaluatePRTargetFresh(t.Context(), "octocat/hello-world", &PullRequestInfo{BaseRef: "feature", DefaultBranch: "main"}, []string{"SchemaBot"})
	require.NoError(t, err)
	assert.Equal(t, PRTargetEligibility{Target: "feature", Eligible: true, ClassicProtectionUnreadable: true}, decision)
	assert.Contains(t, logs.String(), "level=WARN")
	assert.Contains(t, logs.String(), "lacks Administration: read")
	assert.Contains(t, logs.String(), "repo=octocat/hello-world")
	assert.Contains(t, logs.String(), "effective_target=feature")
}
