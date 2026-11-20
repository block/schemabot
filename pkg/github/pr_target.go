package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	// ClassicProtectionUnreadable reports that the target was treated as
	// enforced because the App cannot read its classic branch protection.
	ClassicProtectionUnreadable bool
}

// errClassicProtectionUnreadable marks a classic protection read that GitHub
// refused because the App lacks the Administration: read permission.
var errClassicProtectionUnreadable = errors.New("GitHub App lacks the Administration: read permission")

// branchPolicy is the required-check policy read for one repository branch.
// classicUnreadable means classic protection could not be read, so contexts
// holds only what the active rulesets require.
type branchPolicy struct {
	contexts          []string
	classicUnreadable bool
}

func (p branchPolicy) clone() branchPolicy {
	return branchPolicy{contexts: slices.Clone(p.contexts), classicUnreadable: p.classicUnreadable}
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

func evaluatePRTarget(ctx context.Context, repo string, info *PullRequestInfo, checkBases []string, readPolicy func(context.Context, string, string) (branchPolicy, error)) (PRTargetEligibility, error) {
	target, err := info.EffectiveTarget()
	if err != nil {
		return PRTargetEligibility{}, err
	}
	result := PRTargetEligibility{Target: target, Eligible: target == info.DefaultBranch}
	if result.Eligible {
		return result, nil
	}
	policy, err := readPolicy(ctx, repo, target)
	if err != nil {
		return result, fmt.Errorf("resolve required checks for %s branch %q: %w", repo, target, err)
	}
	if requiresAnyCheck(policy.contexts, checkBases) {
		result.Eligible = true
		return result, nil
	}
	// Without classic protection, SchemaBot cannot prove the branch does not
	// require its check. Enforce rather than exclude, as before target
	// enforcement existed.
	if policy.classicUnreadable {
		result.Eligible = true
		result.ClassicProtectionUnreadable = true
	}
	return result, nil
}

// requiresAnyCheck reports whether a required context is one of checkBases,
// either exactly or as "<base> (<environment>)".
func requiresAnyCheck(contexts, checkBases []string) bool {
	for _, name := range contexts {
		for _, base := range checkBases {
			if base != "" && (name == base || (strings.HasPrefix(name, base+" (") && strings.HasSuffix(name, ")"))) {
				return true
			}
		}
	}
	return false
}

func (ic *InstallationClient) requiredChecksForBranch(ctx context.Context, repo, branch string) (branchPolicy, error) {
	if err := ctx.Err(); err != nil {
		return branchPolicy{}, err
	}
	// GitHub repository names are case-insensitive, and ref names are exact.
	key := strings.ToLower(repo) + "\x00" + branch
	if policy, ok := ic.branchPolicies.get(key); ok {
		return policy, nil
	}

	// Bound the combined ruleset and classic lookup independently of the
	// callers. Each waiter may cancel while the shared GitHub read continues.
	flight := ic.branchPolicyReads.DoChan(key, func() (any, error) {
		// A successful read may have landed between the first cache miss
		// and the start of this flight. Apply acceptance uses a different path.
		if policy, ok := ic.branchPolicies.get(key); ok {
			return policy, nil
		}

		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sharedFetchTimeout)
		defer cancel()
		policy, err := ic.fetchRequiredChecks(fetchCtx, repo, branch)
		if err != nil {
			return nil, err
		}
		ic.branchPolicies.put(key, policy)
		return policy, nil
	})
	select {
	case <-ctx.Done():
		return branchPolicy{}, ctx.Err()
	case result := <-flight:
		if result.Err != nil {
			return branchPolicy{}, result.Err
		}
		// Each waiter owns its result independently of the other waiters.
		return result.Val.(branchPolicy).clone(), nil
	}
}

func (ic *InstallationClient) fetchRequiredChecks(ctx context.Context, repo, branch string) (branchPolicy, error) {
	owner, name := splitRepo(repo)
	var contexts []string
	for page := 1; page != 0; {
		rules, resp, err := ic.client.Repositories.GetRulesForBranch(ctx, owner, name, url.PathEscape(branch), &gh.ListOptions{Page: page, PerPage: 100})
		if err != nil {
			return branchPolicy{}, fmt.Errorf("read active branch rules: %w", classifyGitHubAPIError(err))
		}
		if rules == nil {
			return branchPolicy{}, fmt.Errorf("active branch rules response is missing")
		}
		for _, rule := range rules.RequiredStatusChecks {
			if rule == nil {
				return branchPolicy{}, fmt.Errorf("active branch rules returned an empty required-check rule")
			}
			if rule.Parameters.RequiredStatusChecks == nil {
				return branchPolicy{}, fmt.Errorf("active branch rules omitted required check contexts")
			}
			for _, check := range rule.Parameters.RequiredStatusChecks {
				if check == nil {
					return branchPolicy{}, fmt.Errorf("active branch rules returned an empty required check")
				}
				if check.Context == "" {
					return branchPolicy{}, fmt.Errorf("active branch rules returned a required check without a context")
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
	if errors.Is(err, errClassicProtectionUnreadable) {
		// Logged once per GitHub read, so cached evaluations stay quiet.
		if ic.logger != nil {
			ic.logger.Warn("classic branch protection is unreadable because the GitHub App lacks Administration: read; SchemaBot plans and publishes checks for PRs into this branch",
				"repo", repo, "branch", branch)
		}
		return branchPolicy{contexts: contexts, classicUnreadable: true}, nil
	}
	if err != nil {
		return branchPolicy{}, err
	}
	return branchPolicy{contexts: append(contexts, classic...)}, nil
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
		Errors []graphQLError `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode classic branch protection: %w", err)
	}
	if classicProtectionForbidden(result.Errors) {
		return nil, fmt.Errorf("read classic branch protection: %w", errClassicProtectionUnreadable)
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

// classicProtectionForbidden reports whether GitHub refused every part of the
// classic protection query for lack of permission. Any other error, alone or
// mixed in, stays an error.
func classicProtectionForbidden(errs []graphQLError) bool {
	if len(errs) == 0 {
		return false
	}
	for _, e := range errs {
		if e.Type != "FORBIDDEN" {
			return false
		}
	}
	return true
}

// graphQLError is one entry of a GraphQL response's errors list.
type graphQLError struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}
