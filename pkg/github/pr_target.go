package github

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/metrics"
	gh "github.com/google/go-github/v86/github"
)

// pullRequestWithStack preserves native stack metadata not yet modeled by go-github.
// Read it in the same request as the direct base so the two describe one snapshot.
type pullRequestWithStack struct {
	gh.PullRequest
	Stack *struct {
		Base struct {
			Ref string `json:"ref"`
		} `json:"base"`
	} `json:"stack"`
}

// EffectiveTarget resolves the ultimate target of a native stack, or the direct
// base of an ordinary PR. Incomplete metadata is uncertainty, never exclusion.
func (pri *PullRequestInfo) EffectiveTarget() (string, error) {
	if pri.BaseRef == "" {
		return "", fmt.Errorf("pull request reports no base branch")
	}
	if pri.DefaultBranch == "" {
		return "", fmt.Errorf("pull request reports no repository default branch")
	}
	if pri.NativeStack {
		if pri.StackBaseRef == "" {
			return "", fmt.Errorf("native stack reports no ultimate base branch")
		}
		return pri.StackBaseRef, nil
	}
	return pri.BaseRef, nil
}

// PRTargetEligibility describes the branch policy, independently of schema discovery.
type PRTargetEligibility struct {
	Target   string
	Eligible bool
}

// EvaluatePRTarget enforces the default branch and branches whose active rules
// require this deployment's aggregate or promotion check. Names are matched
// conservatively even when GitHub pins a different App: a pin never permits
// silently omitting a required check.
func (ic *InstallationClient) EvaluatePRTarget(ctx context.Context, repo string, info *PullRequestInfo, checkBases []string) (PRTargetEligibility, error) {
	return evaluatePRTarget(ctx, repo, info, checkBases, ic.requiredChecksForBranch)
}

// EvaluatePRTargetFresh uses live policy reads before accepting an apply.
// It bypasses the cache and any background reads already in progress.
func (ic *InstallationClient) EvaluatePRTargetFresh(ctx context.Context, repo string, info *PullRequestInfo, checkBases []string) (PRTargetEligibility, error) {
	fetchCtx, cancel := context.WithTimeout(ctx, sharedFetchTimeout)
	defer cancel()
	return evaluatePRTarget(fetchCtx, repo, info, checkBases, ic.fetchRequiredChecks)
}

func evaluatePRTarget(ctx context.Context, repo string, info *PullRequestInfo, checkBases []string, readPolicy func(context.Context, string, string) ([]string, error)) (PRTargetEligibility, error) {
	target, err := info.EffectiveTarget()
	if err != nil {
		return PRTargetEligibility{}, err
	}
	result := PRTargetEligibility{Target: target, Eligible: target == info.DefaultBranch}
	if result.Eligible {
		return result, nil
	}
	contexts, err := readPolicy(ctx, repo, target)
	if err != nil {
		return result, fmt.Errorf("resolve required checks for %s branch %q: %w", repo, target, err)
	}
	for _, name := range contexts {
		for _, base := range checkBases {
			if base != "" && (name == base || (strings.HasPrefix(name, base+" (") && strings.HasSuffix(name, ")"))) {
				result.Eligible = true
				return result, nil
			}
		}
	}
	return result, nil
}

func (ic *InstallationClient) requiredChecksForBranch(ctx context.Context, repo, branch string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// GitHub repository names are case-insensitive, and ref names are exact.
	key := strings.ToLower(repo) + "\x00" + branch
	if names, ok := ic.branchPolicies.get(key); ok {
		return names, nil
	}

	// Bound the combined ruleset and classic lookup independently of the
	// callers. Each waiter may cancel while the shared GitHub read continues.
	flight := ic.branchPolicyReads.DoChan(key, func() (any, error) {
		// A successful read may have landed between the first cache miss
		// and the start of this flight. Apply acceptance uses a different path.
		if names, ok := ic.branchPolicies.get(key); ok {
			return names, nil
		}

		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sharedFetchTimeout)
		defer cancel()
		names, err := ic.fetchRequiredChecks(fetchCtx, repo, branch)
		if err != nil {
			return nil, err
		}
		ic.branchPolicies.put(key, names)
		return names, nil
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-flight:
		if result.Err != nil {
			return nil, result.Err
		}
		// Each waiter owns its result independently of the other waiters.
		return slices.Clone(result.Val.([]string)), nil
	}
}

func (ic *InstallationClient) fetchRequiredChecks(ctx context.Context, repo, branch string) ([]string, error) {
	owner, name := splitRepo(repo)
	var contexts []string
	for page := 1; page != 0; {
		rules, resp, err := ic.client.Repositories.GetRulesForBranch(ctx, owner, name, url.PathEscape(branch), &gh.ListOptions{Page: page, PerPage: 100})
		if err != nil {
			return nil, fmt.Errorf("read active branch rules: %w", classifyGitHubAPIError(err))
		}
		if rules == nil {
			return nil, fmt.Errorf("active branch rules response is missing")
		}
		for _, rule := range rules.RequiredStatusChecks {
			if rule == nil {
				return nil, fmt.Errorf("active branch rules returned an empty required-check rule")
			}
			if rule.Parameters.RequiredStatusChecks == nil {
				return nil, fmt.Errorf("active branch rules omitted required check contexts")
			}
			for _, check := range rule.Parameters.RequiredStatusChecks {
				if check == nil {
					return nil, fmt.Errorf("active branch rules returned an empty required check")
				}
				if check.Context == "" {
					return nil, fmt.Errorf("active branch rules returned a required check without a context")
				}
				contexts = append(contexts, check.Context)
			}
		}
		page = 0
		if resp != nil {
			page = resp.NextPage
		}
	}
	classic, err := ic.classicRequiredChecks(ctx, repo, branch)
	if err != nil {
		return nil, err
	}
	return append(contexts, classic...), nil
}

// A successful GraphQL null branchProtectionRule proves there is no classic
// protection. REST's generic 404 cannot distinguish that from missing access.
func (ic *InstallationClient) classicRequiredChecks(ctx context.Context, repo, branch string) ([]string, error) {
	owner, name := splitRepo(repo)
	payload, err := json.Marshal(map[string]any{
		"query":     `query($owner: String!, $name: String!, $ref: String!) { repository(owner: $owner, name: $name) { ref(qualifiedName: $ref) { name branchProtectionRule { requiresStatusChecks requiredStatusCheckContexts } } } }`,
		"variables": map[string]string{"owner": owner, "name": name, "ref": "refs/heads/" + branch},
	})
	if err != nil {
		return nil, fmt.Errorf("encode branch protection query: %w", err)
	}
	ctx = withGitHubRateLimitContext(ctx, metrics.GitHubOperationGraphQLBranchProtection, repo)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ic.graphQLURL(), bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("build branch protection query: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := ic.client.Client().Do(req)
	if err != nil {
		return nil, fmt.Errorf("read classic branch protection: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("read classic branch protection: %s", resp.Status)
	}
	var result struct {
		Data struct {
			Repository *struct {
				Ref *struct {
					Name                 string `json:"name"`
					BranchProtectionRule *struct {
						RequiresStatusChecks        *bool     `json:"requiresStatusChecks"`
						RequiredStatusCheckContexts *[]string `json:"requiredStatusCheckContexts"`
					} `json:"branchProtectionRule"`
				} `json:"ref"`
			} `json:"repository"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode classic branch protection: %w", err)
	}
	if len(result.Errors) > 0 {
		return nil, fmt.Errorf("read classic branch protection: %s", result.Errors[0].Message)
	}
	if result.Data.Repository == nil {
		return nil, fmt.Errorf("classic branch protection query returned no repository")
	}
	ref := result.Data.Repository.Ref
	if ref == nil {
		return nil, fmt.Errorf("classic branch protection query returned no branch")
	}
	if ref.Name != branch {
		return nil, fmt.Errorf("classic branch protection query returned branch %q, expected %q", ref.Name, branch)
	}
	if ref.BranchProtectionRule == nil {
		return nil, nil
	}
	if ref.BranchProtectionRule.RequiresStatusChecks == nil {
		return nil, fmt.Errorf("classic branch protection query omitted required status check policy")
	}
	if !*ref.BranchProtectionRule.RequiresStatusChecks {
		return nil, nil
	}
	if ref.BranchProtectionRule.RequiredStatusCheckContexts == nil {
		return nil, fmt.Errorf("classic branch protection query omitted required check contexts")
	}
	return *ref.BranchProtectionRule.RequiredStatusCheckContexts, nil
}
