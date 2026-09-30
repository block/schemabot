package webhook

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/hmarr/codeowners"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// ReviewGateResult contains the outcome of a review gate check.
type ReviewGateResult struct {
	Approved bool
	// OperatorReviewers are the database's own operator principals; the
	// review-required comment leads with them in their own section.
	OperatorReviewers []string
	// OtherReviewers are the broader principals whose approval also satisfies
	// the gate — global admins, then repo admins, then codeowners.
	OtherReviewers []string
	PRAuthor       string
}

// enforceReviewGate runs the review gate check and posts the appropriate comment if blocked.
// Returns blocked=true when the gate blocks on the merits — the PR lacks an
// approval from a configured review-policy principal (caller should return).
// A gate evaluation failure (a GitHub read inside checkReviewGate, or a review
// policy the gate cannot resolve) stops the command (fail closed) and is
// returned as an error, not a block: the approval state could not be
// determined, so the outcome is not the command's answer and a durable driver
// may re-drive it. Policy-shape errors in that class (for example a review
// policy with no configured reviewers) cannot succeed on a re-drive, but they
// are bounded by the driver's retry budget, so the gate does not maintain a
// separate taxonomy for them. suppressRetryComments silences the
// evaluation-failure comment on durable attempts, where the driver retries
// and posts the single terminal answer instead; merit blocks always comment.
func (h *Handler) enforceReviewGate(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, installationID int64, schemaResult *ghclient.SchemaRequestResult, environment, requestedBy, commandName string, suppressRetryComments bool) (blocked bool, err error) {
	gateResult, err := h.checkReviewGate(ctx, client, repo, pr, schemaResult.Database, schemaResult.SchemaPath, schemaResult.SchemaLinkPath)
	if err != nil {
		h.logger.Error("review gate check failed", "repo", repo, "pr", pr,
			"database", schemaResult.Database, "environment", environment,
			"command", commandName, "error", err)
		if !suppressRetryComments {
			h.postCommandError(repo, pr, installationID, commandName, environment, requestedBy, reviewGateErrorDetail(err))
		}
		return false, fmt.Errorf("review gate check %s#%d: %w", repo, pr, err)
	}
	if gateResult != nil && !gateResult.Approved {
		h.postComment(repo, pr, installationID, templates.RenderReviewRequired(templates.ReviewGateData{
			Database:          schemaResult.Database,
			Environment:       environment,
			RequestedBy:       requestedBy,
			OperatorReviewers: gateResult.OperatorReviewers,
			OtherReviewers:    gateResult.OtherReviewers,
			PRAuthor:          gateResult.PRAuthor,
		}))
		return true, nil
	}
	return false, nil
}

// reviewGateErrorDetail builds the PR-facing detail for a review gate
// evaluation failure. The error is used only to classify the failure — its
// text is never rendered, because raw GitHub errors can carry internal detail
// that must not land in PR markdown; operators triage from the server logs.
func reviewGateErrorDetail(err error) string {
	detail := "Review gate check failed; see server logs for details"
	if errors.Is(err, ghclient.ErrTeamMembershipUnreadable) {
		detail += ". If approval is granted through a GitHub team, verify the GitHub App can read organization members and team membership."
	}
	return detail
}

// checkReviewGate checks if the PR has approval from a configured review policy principal.
// Returns nil if review gating is disabled (apply proceeds).
// Returns a result with Approved=true if gate passes.
// Returns a result with Approved=false if gate blocks.
// schemaPath is the repo-relative path to the database's schema directory (e.g. "schema/payments");
// schemaLinkPath is the logical environment path when schemaPath was resolved
// through a symlink, or empty.
//
// An approval counts only for the schema it reviewed: it must have been given
// on the PR's head commit, or on an earlier commit from which no schema input
// changed on the way to the head (see approvalCoversHead).
func (h *Handler) checkReviewGate(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, database, schemaPath, schemaLinkPath string) (*ReviewGateResult, error) {
	if !h.isReviewGateEnabled(repo) {
		return nil, nil
	}

	prInfo, err := client.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		return nil, fmt.Errorf("fetch PR info: %w", err)
	}

	reviews, err := client.ListReviews(ctx, repo, pr)
	if err != nil {
		return nil, fmt.Errorf("fetch PR reviews: %w", err)
	}

	policy, err := h.loadReviewGatePolicy(ctx, client, repo, prInfo.BaseRef, database, schemaPath)
	if err != nil {
		return nil, err
	}
	if len(policy.OperatorReviewers) == 0 && len(policy.OtherReviewers) == 0 {
		return nil, fmt.Errorf("review policy has no configured reviewers for database %q", database)
	}

	approvals := ghclient.GetApprovedReviews(reviews)
	h.logger.Info("review gate: fetched reviews",
		"repo", repo, "pr", pr, "database", database,
		"approved_by", ghclient.GetApprovedReviewers(reviews), "pr_author", prInfo.User,
		"head_sha", prInfo.HeadSHA)

	var validApprovals []*ghclient.ReviewInfo
	var validApprovers []string
	for _, approval := range approvals {
		if !strings.EqualFold(approval.User, prInfo.User) {
			validApprovals = append(validApprovals, approval)
			validApprovers = append(validApprovers, approval.User)
		}
	}

	coverage := approvalCoverage{
		repo:        repo,
		pr:          pr,
		database:    database,
		headSHA:     prInfo.HeadSHA,
		schemaPaths: reviewGateSchemaPaths(schemaPath, schemaLinkPath),
		verdicts:    make(map[string]bool),
	}
	var staleApprovers []string
	for _, approval := range validApprovals {
		reviewer := approval.User
		matched, principal, err := policy.Matches(ctx, client, reviewer)
		if err != nil {
			return nil, err
		}
		// A matching reviewer whose approval does not cover the head is treated
		// like a reviewer who has not approved.
		if matched && !h.approvalCoversHead(ctx, client, &coverage, approval) {
			staleApprovers = append(staleApprovers, reviewer)
			continue
		}
		if matched {
			h.logger.Info("review gate: approved",
				"repo", repo, "pr", pr, "database", database,
				"approved_by", reviewer, "matched_principal", principal,
				"approved_sha", approval.CommitID, "head_sha", prInfo.HeadSHA,
				"operator_reviewers", policy.OperatorReviewers,
				"other_reviewers", policy.OtherReviewers)
			return &ReviewGateResult{
				Approved:          true,
				OperatorReviewers: policy.OperatorReviewers,
				OtherReviewers:    policy.OtherReviewers,
				PRAuthor:          prInfo.User,
			}, nil
		}
	}

	h.logger.Info("review gate: blocked",
		"repo", repo, "pr", pr, "database", database,
		"valid_approvers", validApprovers,
		"stale_approvers", staleApprovers, "head_sha", prInfo.HeadSHA,
		"operator_reviewers", policy.OperatorReviewers,
		"other_reviewers", policy.OtherReviewers)
	return &ReviewGateResult{
		Approved:          false,
		OperatorReviewers: policy.OperatorReviewers,
		OtherReviewers:    policy.OtherReviewers,
		PRAuthor:          prInfo.User,
	}, nil
}

// approvalCoverage holds the PR head an approval must cover and caches the
// verdict per approved commit, so reviewers who approved the same commit share
// one comparison.
type approvalCoverage struct {
	repo        string
	pr          int
	database    string
	headSHA     string
	schemaPaths []string
	verdicts    map[string]bool
}

// approvalCoversHead reports whether an approval still stands for the PR head.
// An approval on the head commit covers it. An approval on an earlier commit
// covers it only when GitHub proves the head descends from that commit and no
// schema input — a schema or config file anywhere, or any file under the
// database's schema paths — changed in between. Anything that prevents that
// proof (an unknown commit, a compare error, history rewritten by a
// force-push, a truncated file list) leaves the approval not counting.
func (h *Handler) approvalCoversHead(ctx context.Context, client *ghclient.InstallationClient, c *approvalCoverage, approval *ghclient.ReviewInfo) bool {
	approvedSHA := approval.CommitID
	if c.headSHA == "" || approvedSHA == "" {
		h.logger.Warn("review gate: approval does not count because its commit or the PR head is unknown",
			"repo", c.repo, "pr", c.pr, "database", c.database, "reviewer", approval.User,
			"approved_sha", approvedSHA, "head_sha", c.headSHA)
		return false
	}
	if approvedSHA == c.headSHA {
		return true
	}
	if covers, ok := c.verdicts[approvedSHA]; ok {
		return covers
	}
	covers := h.schemaUnchangedSince(ctx, client, c, approval)
	c.verdicts[approvedSHA] = covers
	return covers
}

func (h *Handler) schemaUnchangedSince(ctx context.Context, client *ghclient.InstallationClient, c *approvalCoverage, approval *ghclient.ReviewInfo) bool {
	files, err := client.FetchChangedFilesBetween(ctx, c.repo, approval.CommitID, c.headSHA)
	if err != nil {
		h.logger.Warn("review gate: approval on an earlier commit does not count because the schema inputs since it could not be compared",
			"repo", c.repo, "pr", c.pr, "database", c.database, "reviewer", approval.User,
			"approved_sha", approval.CommitID, "head_sha", c.headSHA, "error", err)
		return false
	}
	if ghclient.HasSchemaInputFiles(files) || anyFileUnderPaths(files, c.schemaPaths) {
		h.logger.Info("review gate: approval on an earlier commit does not count because schema inputs changed since it",
			"repo", c.repo, "pr", c.pr, "database", c.database, "reviewer", approval.User,
			"approved_sha", approval.CommitID, "head_sha", c.headSHA)
		return false
	}
	h.logger.Info("review gate: approval on an earlier commit counts because no schema input changed since it",
		"repo", c.repo, "pr", c.pr, "database", c.database, "reviewer", approval.User,
		"approved_sha", approval.CommitID, "head_sha", c.headSHA)
	return true
}

// reviewGateSchemaPaths lists the schema paths the gate protects, the same
// paths the base schema freshness guard compares.
func reviewGateSchemaPaths(schemaPath, schemaLinkPath string) []string {
	var paths []string
	for _, p := range []string{schemaPath, schemaLinkPath} {
		if p != "" {
			paths = append(paths, p)
		}
	}
	return paths
}

// anyFileUnderPaths reports whether a changed file, or the path it was renamed
// from, is one of paths or lies beneath one. A root path covers every file.
func anyFileUnderPaths(files []ghclient.PRFile, paths []string) bool {
	for _, f := range files {
		for _, p := range paths {
			if pathWithin(f.Filename, p) || pathWithin(f.PreviousFilename, p) {
				return true
			}
		}
	}
	return false
}

func pathWithin(file, dir string) bool {
	if file == "" {
		return false
	}
	dir = strings.TrimSuffix(dir, "/")
	if dir == "" || dir == "." {
		return true
	}
	return file == dir || strings.HasPrefix(file, dir+"/")
}

type reviewGatePolicy struct {
	AdminTeams         []string
	AdminUsers         []string
	RepoAdminTeams     []string
	RepoAdminUsers     []string
	OperatorTeams      []string
	OperatorUsers      []string
	CodeownerReviewers map[string]struct{}
	// OperatorReviewers and OtherReviewers are the PR-facing reviewer lists on
	// the review-required comment. The database's own operators get their own
	// leading section — they are the reviewers the author should ping — while
	// the broader fallbacks follow in order of scope: global admin principals,
	// then repo admins, then codeowners. A principal configured in more than
	// one tier is listed once, in its most specific tier.
	OperatorReviewers []string
	OtherReviewers    []string
}

func (p reviewGatePolicy) Matches(ctx context.Context, client *ghclient.InstallationClient, reviewer string) (bool, string, error) {
	if len(p.AdminTeams) > 0 {
		matched, principal, err := actorInAnyTeam(ctx, client, p.AdminTeams, reviewer)
		if err != nil {
			return false, "", err
		}
		if matched {
			return true, principal, nil
		}
	}
	if matched, principal := matchedUserPrincipal(p.AdminUsers, reviewer); matched {
		return true, principal, nil
	}
	if len(p.RepoAdminTeams) > 0 {
		matched, principal, err := actorInAnyTeam(ctx, client, p.RepoAdminTeams, reviewer)
		if err != nil {
			return false, "", err
		}
		if matched {
			return true, principal, nil
		}
	}
	if matched, principal := matchedUserPrincipal(p.RepoAdminUsers, reviewer); matched {
		return true, principal, nil
	}
	if len(p.OperatorTeams) > 0 {
		matched, principal, err := actorInAnyTeam(ctx, client, p.OperatorTeams, reviewer)
		if err != nil {
			return false, "", err
		}
		if matched {
			return true, principal, nil
		}
	}
	if matched, principal := matchedUserPrincipal(p.OperatorUsers, reviewer); matched {
		return true, principal, nil
	}
	if _, ok := p.CodeownerReviewers[strings.ToLower(reviewer)]; ok {
		return true, "CODEOWNERS", nil
	}
	return false, "", nil
}

func (h *Handler) loadReviewGatePolicy(ctx context.Context, client *ghclient.InstallationClient, repo, baseRef, database, schemaPath string) (reviewGatePolicy, error) {
	if h.service == nil || h.service.Config() == nil {
		return reviewGatePolicy{}, fmt.Errorf("server config is unavailable")
	}
	config := h.service.Config()
	reviewPolicy := config.ReviewPolicy
	repoAdminTeams, repoAdminUsers := config.RepoAdmins(repo)
	policy := reviewGatePolicy{
		AdminTeams:         reviewPolicy.AdminTeams,
		AdminUsers:         reviewPolicy.AdminUsers,
		RepoAdminTeams:     repoAdminTeams,
		RepoAdminUsers:     repoAdminUsers,
		CodeownerReviewers: make(map[string]struct{}),
	}
	appendOperatorReviewers := func(values ...[]string) {
		for _, group := range values {
			for _, value := range group {
				policy.OperatorReviewers = appendUniqueString(policy.OperatorReviewers, value)
			}
		}
	}
	// A principal already listed as an operator is not repeated in the
	// fallback section.
	appendOtherReviewers := func(values ...[]string) {
		for _, group := range values {
			for _, value := range group {
				if containsFold(policy.OperatorReviewers, value) {
					continue
				}
				policy.OtherReviewers = appendUniqueString(policy.OtherReviewers, value)
			}
		}
	}

	if config.ReviewPolicyIncludesDatabaseOperators() {
		dbConfig := config.Database(database)
		if dbConfig == nil {
			return reviewGatePolicy{}, fmt.Errorf("database %q is not configured for review policy", database)
		}
		policy.OperatorTeams = dbConfig.OperatorTeams
		policy.OperatorUsers = dbConfig.OperatorUsers
		appendOperatorReviewers(dbConfig.OperatorTeams, dbConfig.OperatorUsers)
	}

	appendOtherReviewers(reviewPolicy.AdminTeams, reviewPolicy.AdminUsers, repoAdminTeams, repoAdminUsers)

	if reviewPolicy.IncludeCodeowners {
		owners, err := matchReviewGateCodeowners(ctx, client, repo, baseRef, schemaPath)
		if err != nil {
			return reviewGatePolicy{}, err
		}
		for _, owner := range owners {
			if ghclient.IsTeamOwner(owner) {
				org, slug := ghclient.TeamParts(owner)
				members, err := client.ListTeamMembers(ctx, org, slug)
				if err != nil {
					return reviewGatePolicy{}, fmt.Errorf("expand CODEOWNERS team %s: %w", owner.String(), err)
				}
				for _, member := range members {
					policy.CodeownerReviewers[strings.ToLower(member)] = struct{}{}
				}
			} else {
				policy.CodeownerReviewers[strings.ToLower(owner.Value)] = struct{}{}
			}
		}
		appendOtherReviewers(ghclient.OwnerNames(owners))
	}

	return policy, nil
}

func matchReviewGateCodeowners(ctx context.Context, client *ghclient.InstallationClient, repo, baseRef, schemaPath string) ([]codeowners.Owner, error) {
	ruleset, err := client.FetchCodeownersRuleset(ctx, repo, baseRef)
	if err != nil {
		return nil, fmt.Errorf("fetch CODEOWNERS: %w", err)
	}
	if ruleset == nil {
		return nil, nil
	}

	var owners []codeowners.Owner
	if schemaPath != "" {
		matchPath := strings.TrimSuffix(schemaPath, "/") + "/.schema"
		owners, err = ghclient.MatchCodeownersPath(ruleset, matchPath)
		if err != nil {
			return nil, fmt.Errorf("match CODEOWNERS for %s: %w", schemaPath, err)
		}
	}
	if owners == nil {
		owners = ghclient.OwnersFromRuleset(ruleset)
	}
	return owners, nil
}

// isReviewGateEnabled checks server config for the review gate toggle.
func (h *Handler) isReviewGateEnabled(repo string) bool {
	enabled := h.service != nil && h.service.Config().ReviewPolicyEnabled()
	h.logger.Debug("review gate: server config", "repo", repo, "enabled", enabled)
	return enabled
}

func appendUniqueString(values []string, value string) []string {
	if containsFold(values, value) {
		return values
	}
	return append(values, value)
}

func containsFold(values []string, value string) bool {
	for _, existing := range values {
		if strings.EqualFold(existing, value) {
			return true
		}
	}
	return false
}
