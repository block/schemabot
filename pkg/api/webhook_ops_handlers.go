package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/caller"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

const (
	// webhookOpsRequestTimeout bounds the webhook operator endpoints, which
	// can crawl every open PR and routinely need far longer than the
	// server-wide write timeout allows.
	webhookOpsRequestTimeout = 15 * time.Minute

	// webhookScanPRPageSize bounds one scan request to a single page of open
	// PRs; callers continue via next_page.
	webhookScanPRPageSize = 30
)

// webhookOpsRequestError marks a failure caused by the request itself
// (validation, unknown app name), as opposed to a server-side or GitHub-side
// failure while executing it — the two need different HTTP status codes so
// operators know whether to fix the request or investigate the server.
type webhookOpsRequestError struct{ err error }

func (e *webhookOpsRequestError) Error() string { return e.err.Error() }
func (e *webhookOpsRequestError) Unwrap() error { return e.err }

func webhookOpsRequestErrorf(format string, args ...any) error {
	return &webhookOpsRequestError{err: fmt.Errorf(format, args...)}
}

// requireRepoFullName refuses a repository an operator endpoint could never
// answer for. The shape is checked up front rather than left to the first thing
// that trips over it: a repository that is not an owner/name pair is the
// caller's mistake, and letting the installation lookup fail on it reports a
// server error for a request that was never answerable. Every endpoint taking a
// repository asks through here, so the same typo reads the same way on all of
// them; the endpoints where a repository is an optional filter rather than the
// subject decide for themselves whether it was given, and ask only then.
func requireRepoFullName(repo string) error {
	if repo == "" {
		return webhookOpsRequestErrorf("repo is required")
	}
	if !caller.IsRepoFullName(repo) {
		return webhookOpsRequestErrorf("repo %q is not an owner/name pair", repo)
	}
	return nil
}

// canonicalEnvironment folds an environment the way everything it will be
// compared against is already folded: configured names are validated lowercase,
// stored check rows canonicalize theirs on write and on lookup, and the Check
// Run name is interpolated from that same folded value. An operator who types
// "Production" is naming the environment the instance handles, not a different
// one. Every operator endpoint taking an environment folds through here, so one
// spelling cannot be answered on one command and refused on another.
func canonicalEnvironment(environment string) string {
	return storage.CanonicalKey(strings.TrimSpace(environment))
}

// canonicalRepo trims an operator-supplied repository. The owner/name shape
// check tolerates surrounding whitespace — a trailing space lands inside the
// name half and the pair still parses — but every use of the value afterwards
// is an exact comparison, against a configured repository or against the one a
// delivery names. A padded repository therefore clears the guard and then
// matches nothing, which is the silent answer the guard exists to prevent.
func canonicalRepo(repo string) string {
	return strings.TrimSpace(repo)
}

// requireNarrowableEnvironment refuses an environment this instance cannot
// narrow a checks query by.
//
// An instance with no configured environments publishes one check that is not
// environment-scoped and stores one aggregate beside it, so there is no
// environment to select: narrowing by any name looks for a Check Run that
// instance never creates. An instance that does scope its checks refuses a name
// it does not handle for the same reason. Either way the query would be
// answered from a name that cannot exist — every pull request reported as
// missing its check, and on a sweep that acts, every one of them re-planned.
//
// An empty environment is always allowed; it is what asks for the unscoped check.
func requireNarrowableEnvironment(cfg *ServerConfig, environment string) error {
	if environment == "" {
		return nil
	}
	if len(cfg.AllowedEnvironments) == 0 {
		return webhookOpsRequestErrorf("this instance publishes one check for every environment, so there is no environment to narrow to; omit the environment")
	}
	if !cfg.IsEnvironmentAllowed(environment) {
		return webhookOpsRequestErrorf("environment %q is not one this instance handles", environment)
	}
	return nil
}

// extendWebhookOpsDeadline lifts the server-wide write timeout for a webhook
// operator request and returns a context bounded to the same budget, so the
// crawl can outlive the default timeout without running unbounded.
func (s *Service) extendWebhookOpsDeadline(w http.ResponseWriter, r *http.Request) (context.Context, context.CancelFunc) {
	return s.extendOperatorWriteDeadline(w, r, webhookOpsRequestTimeout)
}

// writeWebhookOpsError answers a failed webhook operator request. A refusal of
// the request itself keeps its own text. Any other failure answers a fixed
// message, because its cause can be a GitHub, storage, or SQL driver error
// carrying addresses or credentials; the cause goes to the server log with the
// operation, the repository, and attrs, the identifiers the caller has in scope.
func (s *Service) writeWebhookOpsError(w http.ResponseWriter, operation, repo string, err error, attrs ...any) {
	var reqErr *webhookOpsRequestError
	if errors.As(err, &reqErr) {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	logAttrs := append([]any{"operation", operation, "repo", repo}, attrs...)
	logAttrs = append(logAttrs, "error", err)
	if cause, ok := operatorFixableWebhookOpsCause(err); ok {
		s.logger.Warn("webhook operator request failed: "+cause.reason, logAttrs...)
	} else {
		s.logger.Error("webhook operator request failed", logAttrs...)
	}
	s.writeError(w, http.StatusInternalServerError, webhookOpsFailureMessage(operation, repo, err))
}

// webhookOpsFailureMessage is the 500 body of a webhook operator request that
// failed inside SchemaBot. The cause stays in the server log. These requests
// read state or replay what a webhook delivery would have done, so they can be
// repeated. A cause the operator can fix and a retry cannot is named instead,
// with what to change.
func webhookOpsFailureMessage(operation, repo string, err error) string {
	subject := strings.ReplaceAll(operation, "_", " ") + " failed"
	if repo != "" {
		subject += fmt.Sprintf(" for repository %q", repo)
	}
	if cause, ok := operatorFixableWebhookOpsCause(err); ok {
		return subject + ": " + cause.remedy
	}
	return subject + ": SchemaBot hit an internal error. Retry in a moment. " + retryFailureTail
}

// errCheckRunBackfillUnavailable reports a synthesize request to a server that
// runs no GitHub webhook pipeline to replay deliveries through.
var errCheckRunBackfillUnavailable = errors.New("check run backfill is unavailable: no GitHub webhook runtime is configured")

// webhookOpsFixableCause is a webhook operator failure the operator fixes and
// a retry does not: reason is the server log's wording, remedy the response's.
type webhookOpsFixableCause struct {
	reason string
	remedy string
}

// operatorFixableWebhookOpsCause reports whether err's cause is one the
// operator fixes, and what they should do. Each remedy is text SchemaBot wrote
// and carries nothing from the cause beyond configured names.
func operatorFixableWebhookOpsCause(err error) (webhookOpsFixableCause, bool) {
	if errors.Is(err, ErrRepoNotConfigured) {
		return webhookOpsFixableCause{
			reason: "the repository is not declared in the repos config",
			remedy: "the repository is not declared in this server's repos config. " +
				"Name a repository this server serves, or add it to the repos config.",
		}, true
	}
	if appErr, ok := errors.AsType[*RepoGitHubAppNotConfiguredError](err); ok {
		remedy := "the repos config gives this repository no github_app. " +
			"Set its github_app to one of the apps in the server config."
		if appErr.App != "" {
			remedy = fmt.Sprintf("the repos config sets this repository's github_app to %q, which is not one of the apps in the server config. ", appErr.App) +
				"Set its github_app to a configured App."
		}
		return webhookOpsFixableCause{reason: "the repository's github_app is not a configured App", remedy: remedy}, true
	}
	if errors.Is(err, ghclient.ErrAppNotInstalled) {
		return webhookOpsFixableCause{
			reason: "the GitHub App is not installed on the repository",
			remedy: "the GitHub App that serves this repository is not installed on it. Install the App on the repository.",
		}, true
	}
	if errors.Is(err, errCheckRunBackfillUnavailable) {
		return webhookOpsFixableCause{
			reason: "no GitHub webhook runtime is configured on this server",
			remedy: "this server has no GitHub App configured, so it cannot replay webhook deliveries to create Check Runs. " +
				"Configure a GitHub App on this server, or send the request to the SchemaBot server that receives this repository's webhooks.",
		}, true
	}
	if text, ok := authoredMessage(err); ok {
		return webhookOpsFixableCause{reason: "the request names something the operator must correct", remedy: text}, true
	}
	return webhookOpsFixableCause{}, false
}

type ChecksScanRequest = apitypes.ChecksScanRequest
type ChecksScanResponse = apitypes.ChecksScanResponse
type ChecksReposResponse = apitypes.ChecksReposResponse
type MissingCheckPR = apitypes.MissingCheckPR
type MissingMergeQueueCheck = apitypes.MissingMergeQueueCheck
type StuckCheckPR = apitypes.StuckCheckPR
type IncompleteCheckRun = apitypes.IncompleteCheckRun

// CheckRunBackfiller replays the auto-plan flow for a PR to recreate missing
// Check Runs, exactly as the check-creating webhook delivery would have. The
// webhook handler implements it; the API service holds it so the checks
// operator endpoints can reach the webhook pipeline.
type CheckRunBackfiller interface {
	BackfillPRCheckRuns(ctx context.Context, repo string, pr int, installationID int64, opts CheckRunBackfillOptions) (CheckRunBackfillResult, error)
	// BackfillMergeQueueCheck posts the passing merge-queue checks in scope
	// that a merge group head is still missing, on a head the caller has
	// verified the queue is still testing.
	BackfillMergeQueueCheck(ctx context.Context, repo string, installationID int64, headSHA string, scope MergeQueueCheckScope) (string, error)
}

// MergeQueueCheckScope limits a merge queue check backfill to the checks an
// operator asked about. Environment keeps the checks for one environment, and
// CheckName keeps the check with that exact name. Empty fields keep every
// check the merge_group delivery would post.
type MergeQueueCheckScope struct {
	Environment string
	CheckName   string
}

// CheckRunBackfillOptions controls how much work one PR's backfill may start.
type CheckRunBackfillOptions struct {
	// Plan runs the auto-plans of a PR that changes managed schema. Without
	// it that PR is deferred, and no Check Run or check state is written for it.
	Plan bool
}

// CheckRunBackfillResult is what one PR's backfill did.
type CheckRunBackfillResult struct {
	Outcome string
	// Deferred is true when the PR changes managed schema and was not planned
	// because the request did not ask for plans.
	Deferred bool
	// Databases are the managed databases a deferred PR would plan.
	Databases []string
	// ChangedFiles counts the files a deferred PR proposes, as a size hint.
	ChangedFiles int
}

type ChecksSynthesizeRequest = apitypes.ChecksSynthesizeRequest
type ChecksSynthesizeResponse = apitypes.ChecksSynthesizeResponse
type ChecksSynthesizeResult = apitypes.ChecksSynthesizeResult
type ChecksMergeQueueResult = apitypes.ChecksMergeQueueResult

// checksSynthesizeMaxPRsPerRequest bounds one synthesize request. A PR's plans
// run asynchronously and only when the request asks for them, so a chunk pays
// for discovery alone.
const checksSynthesizeMaxPRsPerRequest = 10

// checksSynthesizeMaxMergeQueueHeadsPerRequest bounds the merge group heads in
// one synthesize request. Each costs a few check reads and writes.
const checksSynthesizeMaxMergeQueueHeadsPerRequest = 20

// mergeQueueHeadGoneOutcome is the outcome for a requested merge group head
// the queue is no longer testing. Nothing is posted on it.
const mergeQueueHeadGoneOutcome = "skipped: no longer in the merge queue"

// The texts a synthesize or scan response carries for a failure within it.
// The cause can be a GitHub, storage, or SQL driver error carrying addresses
// or credentials, so it goes to the server log beside the repository and the
// PR or head, and the response says only what failed. The CLI shows each
// after "failed: " or in its sentence about the merge queue.
const (
	prBackfillFailedText         = "SchemaBot could not recreate this PR's Check Runs; see the server log for the cause"
	mergeQueueBackfillFailedText = "SchemaBot could not post the merge queue check on this head; see the server log for the cause"
	mergeQueueUnlistedText       = "SchemaBot could not list the merge queue to verify this head, so nothing was posted; see the server log for the cause"
	openPRHeadUncheckedText      = "SchemaBot could not check whether this head is also an open PR's head, so nothing was posted; see the server log for the cause"
	mergeQueueUnreadText         = "SchemaBot could not read it; see the server log for the cause"
)

// checksDisabledSkipOutcome is the per-PR outcome when a synthesize request
// names a repository whose Check Run publishing is turned off. A skip, not an
// error: a fleet sweep that includes a disabled repo should keep going.
const checksDisabledSkipOutcome = "skipped: Check Runs are disabled for this repository (enable_checks: false)"

func (s *Service) handleChecksSynthesize(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeDirectAdminWrite(w, r, "checks_synthesize") {
		return
	}
	var req ChecksSynthesizeRequest
	if err := decodeStrictJSONBody(r, &req); err != nil {
		s.writeBodyDecodeError(w, err)
		return
	}
	ctx, cancel := s.extendWebhookOpsDeadline(w, r)
	defer cancel()
	response, err := executeChecksSynthesize(ctx, s.config, s.checkRunBackfiller, req, s.logger)
	if err != nil {
		s.writeWebhookOpsError(w, "checks_synthesize", canonicalRepo(req.Repo), err,
			"environment", req.Environment, "prs", req.PRs, "merge_queue_heads", req.MergeQueueHeads)
		return
	}
	s.writeJSON(w, http.StatusOK, response)
}

func executeChecksSynthesize(ctx context.Context, cfg *ServerConfig, backfiller CheckRunBackfiller, req ChecksSynthesizeRequest, logger *slog.Logger) (*ChecksSynthesizeResponse, error) {
	if cfg == nil {
		return nil, fmt.Errorf("server config is nil")
	}
	if err := validateChecksSynthesizeRequest(cfg, &req); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(os.Stderr, nil))
	}
	// Backfill exists to create Check Runs; on a repo with publishing turned
	// off it could only replay auto-plans whose checks the publisher refuses,
	// leaving plan comments as the sole side effect. Skip per PR rather than
	// erroring so a fleet sweep that includes a disabled repo keeps going.
	if !cfg.AreChecksEnabled(req.Repo) {
		logger.Info("Check Run backfill skipping repository with checks disabled", "repo", req.Repo, "prs", len(req.PRs))
		response := &ChecksSynthesizeResponse{Repo: req.Repo}
		for _, head := range req.MergeQueueHeads {
			response.MergeQueue = append(response.MergeQueue, ChecksMergeQueueResult{HeadSHA: head, Outcome: checksDisabledSkipOutcome})
		}
		for _, pr := range req.PRs {
			response.Results = append(response.Results, ChecksSynthesizeResult{PR: pr, Outcome: checksDisabledSkipOutcome})
		}
		return response, nil
	}
	if backfiller == nil {
		return nil, errCheckRunBackfillUnavailable
	}
	if req.RateLimitFloorPercent < 0 || req.RateLimitFloorPercent > 99 {
		return nil, webhookOpsRequestErrorf("rate_limit_floor_percent must be between 0 and 99; got %d", req.RateLimitFloorPercent)
	}
	installationClient, installationID, err := resolveRepoInstallationClient(ctx, cfg, req.Repo, logger)
	if err != nil {
		return nil, err
	}
	// Merge groups go first: each one holds every PR queued behind it.
	response := &ChecksSynthesizeResponse{
		Repo:       req.Repo,
		MergeQueue: backfillMergeQueueWithinBudget(ctx, installationClient, backfiller, req, installationID, logger),
		Results:    backfillPRsWithinBudget(ctx, installationClient, backfiller, req, installationID, logger),
	}
	response.RateLimit = gitHubRateLimitSnapshot(ctx, installationClient, logger, "checks synthesize batch")
	return response, nil
}

// validateChecksSynthesizeRequest refuses a malformed synthesize request and
// folds its environment the way the scan folds it, so the environment a scan
// accepted is accepted here too and matches the configured names exactly.
func validateChecksSynthesizeRequest(cfg *ServerConfig, req *ChecksSynthesizeRequest) error {
	if err := requireRepoFullName(req.Repo); err != nil {
		return err
	}
	if len(req.PRs) == 0 && len(req.MergeQueueHeads) == 0 {
		return webhookOpsRequestErrorf("prs or merge_queue_heads is required")
	}
	if len(req.MergeQueueHeads) > checksSynthesizeMaxMergeQueueHeadsPerRequest {
		return webhookOpsRequestErrorf("at most %d merge_queue_heads per request; send the rest in follow-up requests", checksSynthesizeMaxMergeQueueHeadsPerRequest)
	}
	if slices.Contains(req.MergeQueueHeads, "") {
		return webhookOpsRequestErrorf("merge_queue_heads must not contain an empty commit")
	}
	req.Environment = canonicalEnvironment(req.Environment)
	if err := requireNarrowableEnvironment(cfg, req.Environment); err != nil {
		return err
	}
	if len(req.PRs) > checksSynthesizeMaxPRsPerRequest {
		return webhookOpsRequestErrorf("at most %d prs per request; send the rest in follow-up requests", checksSynthesizeMaxPRsPerRequest)
	}
	for _, pr := range req.PRs {
		if pr <= 0 {
			return webhookOpsRequestErrorf("pr numbers must be positive; got %d", pr)
		}
	}
	return nil
}

// coreRateLimitReader reads an installation's remaining core GitHub budget.
type coreRateLimitReader interface {
	CoreRateLimit(ctx context.Context) (remaining, limit int, resetAt time.Time, err error)
}

// backfillPRsWithinBudget backfills req's PRs in order. Before each PR it
// re-reads the GitHub budget, and once the budget is below the request's floor
// it stops: the PR and every one after it come back marked RateLimited, for
// the caller to send again after the reset. The budget is shared with live
// webhook traffic, and one PR's discovery can cost hundreds of requests, so
// checking only between batches lets a single batch drain it.
//
// A budget that cannot be read does not stop the batch. The floor is pacing,
// not a safety gate, and GitHub's own rate limiting still applies underneath.
func backfillPRsWithinBudget(ctx context.Context, rate coreRateLimitReader, backfiller CheckRunBackfiller, req ChecksSynthesizeRequest, installationID int64, logger *slog.Logger) []ChecksSynthesizeResult {
	opts := CheckRunBackfillOptions{Plan: req.Plan}
	results := make([]ChecksSynthesizeResult, 0, len(req.PRs))
	for i, pr := range req.PRs {
		if below, remaining, limit := budgetBelowFloor(ctx, rate, req.RateLimitFloorPercent, logger, req.Repo, pr); below {
			logger.Info("Check Run backfill stopping the batch because the GitHub budget is below the floor; the caller will resend the rest after the reset",
				"repo", req.Repo, "pr", pr, "remaining", remaining, "limit", limit,
				"floor_percent", req.RateLimitFloorPercent, "prs_not_started", len(req.PRs)-i)
			for _, skipped := range req.PRs[i:] {
				results = append(results, ChecksSynthesizeResult{
					PR:          skipped,
					Outcome:     fmt.Sprintf("not started: GitHub budget is below %d%% (%d of %d requests left)", req.RateLimitFloorPercent, remaining, limit),
					RateLimited: true,
				})
			}
			return results
		}
		backfilled, err := backfiller.BackfillPRCheckRuns(ctx, req.Repo, pr, installationID, opts)
		result := ChecksSynthesizeResult{
			PR:           pr,
			Outcome:      backfilled.Outcome,
			Deferred:     backfilled.Deferred,
			Databases:    backfilled.Databases,
			ChangedFiles: backfilled.ChangedFiles,
		}
		if err != nil {
			// Each PR's backfill is independent; report the failure and keep
			// going so one bad PR does not strand the rest of the batch.
			logger.Warn("check run backfill failed for PR", "repo", req.Repo, "pr", pr, "environment", req.Environment, "error", err)
			result.Error = prBackfillFailedText
		}
		results = append(results, result)
	}
	return results
}

// mergeQueueSynthesizeClient is the GitHub surface a merge queue backfill
// reads through before it posts.
type mergeQueueSynthesizeClient interface {
	coreRateLimitReader
	mergeQueueLister
	OpenPullRequestHeadedAt(ctx context.Context, repo, sha string) (int, error)
}

// backfillMergeQueueWithinBudget posts the passing merge-queue check on each
// requested head the queue is still testing, under the same budget floor as
// the PRs.
//
// The check passes unconditionally, which is right only on a commit the queue
// built from PRs whose own checks already passed. A head sent by the caller
// proves nothing about that, so the queue is listed again here and a head it
// no longer holds gets nothing. If the queue cannot be listed, nothing is
// posted (MG-1).
//
// The listing returns only heads GitHub's merge queue itself reports as an
// entry's head commit, so a gh-readonly-queue/ branch the queue did not make
// gets nothing. As a second guard, a head that is also an open PR's head is
// refused, because the passing check would pass that PR's own required check
// too. If that cannot be checked, nothing is posted on it.
func backfillMergeQueueWithinBudget(ctx context.Context, client mergeQueueSynthesizeClient, backfiller CheckRunBackfiller, req ChecksSynthesizeRequest, installationID int64, logger *slog.Logger) []ChecksMergeQueueResult {
	if len(req.MergeQueueHeads) == 0 {
		return nil
	}
	results := make([]ChecksMergeQueueResult, 0, len(req.MergeQueueHeads))
	heads, err := client.ListMergeQueueHeads(ctx, req.Repo)
	if err != nil {
		logger.Warn("merge queue check backfill posting nothing: the merge queue could not be listed to verify the requested heads",
			"repo", req.Repo, "head_shas", req.MergeQueueHeads, "error", err)
		for _, head := range req.MergeQueueHeads {
			results = append(results, ChecksMergeQueueResult{HeadSHA: head, Error: mergeQueueUnlistedText})
		}
		return results
	}
	live := make(map[string]bool, len(heads))
	for _, head := range heads {
		live[head.HeadSHA] = true
	}
	for i, head := range req.MergeQueueHeads {
		if !live[head] {
			logger.Info("merge queue check backfill skipping a head the merge queue is no longer testing", "repo", req.Repo, "head_sha", head)
			results = append(results, ChecksMergeQueueResult{HeadSHA: head, Outcome: mergeQueueHeadGoneOutcome})
			continue
		}
		if below, remaining, limit := budgetBelowFloor(ctx, client, req.RateLimitFloorPercent, logger, req.Repo, 0); below {
			logger.Info("merge queue check backfill stopping because the GitHub budget is below the floor; the caller will resend the rest after the reset",
				"repo", req.Repo, "head_sha", head, "remaining", remaining, "limit", limit,
				"floor_percent", req.RateLimitFloorPercent, "heads_not_started", len(req.MergeQueueHeads)-i)
			for _, skipped := range req.MergeQueueHeads[i:] {
				results = append(results, ChecksMergeQueueResult{
					HeadSHA:     skipped,
					Outcome:     fmt.Sprintf("not started: GitHub budget is below %d%% (%d of %d requests left)", req.RateLimitFloorPercent, remaining, limit),
					RateLimited: true,
				})
			}
			return results
		}
		if refusal := refuseMergeQueueHeadOfOpenPR(ctx, client, req.Repo, head, logger); refusal != "" {
			results = append(results, ChecksMergeQueueResult{HeadSHA: head, Error: refusal})
			continue
		}
		outcome, err := backfiller.BackfillMergeQueueCheck(ctx, req.Repo, installationID, head, MergeQueueCheckScope{Environment: req.Environment, CheckName: req.CheckName})
		result := ChecksMergeQueueResult{HeadSHA: head, Outcome: outcome}
		if err != nil {
			// Each merge group is independent; report the failure and keep
			// going so one bad head does not strand the rest.
			logger.Warn("merge queue check backfill failed for head", "repo", req.Repo, "head_sha", head,
				"environment", req.Environment, "check_name", req.CheckName, "error", err)
			result.Error = mergeQueueBackfillFailedText
		}
		results = append(results, result)
	}
	return results
}

// refuseMergeQueueHeadOfOpenPR returns why a passing merge queue check must
// not be posted on head, or "" when it may be. It refuses a head that is also
// an open PR's head, and a head it could not check.
func refuseMergeQueueHeadOfOpenPR(ctx context.Context, client mergeQueueSynthesizeClient, repo, head string, logger *slog.Logger) string {
	pr, err := client.OpenPullRequestHeadedAt(ctx, repo, head)
	if err != nil {
		logger.Warn("merge queue check backfill posting nothing on a head it could not check against open PR heads",
			"repo", repo, "head_sha", head, "error", err)
		return openPRHeadUncheckedText
	}
	if pr > 0 {
		logger.Warn("merge queue check backfill refusing a head that is also an open PR's head; a passing merge queue check there would pass the PR's own required check",
			"repo", repo, "head_sha", head, "pr", pr)
		return fmt.Sprintf("refused: this commit is also the head of open PR #%d, and a passing merge queue check would pass that PR's required check", pr)
	}
	return ""
}

// budgetBelowFloor reports whether the installation's GitHub budget is below
// floorPct percent of its limit. A floor of zero, or a budget that cannot be
// read, is never below.
func budgetBelowFloor(ctx context.Context, rate coreRateLimitReader, floorPct int, logger *slog.Logger, repo string, pr int) (below bool, remaining, limit int) {
	if floorPct <= 0 {
		return false, 0, 0
	}
	remaining, limit, _, err := rate.CoreRateLimit(ctx)
	if err != nil {
		logger.Warn("failed to read the GitHub budget before backfilling a PR; backfilling it without the floor check",
			"repo", repo, "pr", pr, "error", err)
		return false, 0, 0
	}
	if limit <= 0 {
		logger.Warn("GitHub reported no core budget limit; backfilling the PR without the floor check",
			"repo", repo, "pr", pr, "remaining", remaining, "limit", limit)
		return false, remaining, limit
	}
	return remaining*100 < limit*floorPct, remaining, limit
}

// gitHubRateLimitSnapshot reads the installation's remaining core budget so
// the caller can pace a large backfill instead of starving the live webhook
// path that shares the same budget. The snapshot is advisory: the operation
// that produced the response already succeeded, so a read failure is logged
// and the pacing info omitted rather than failing the request.
func gitHubRateLimitSnapshot(ctx context.Context, client coreRateLimitReader, logger *slog.Logger, operation string) *apitypes.GitHubRateLimit {
	remaining, limit, resetAt, err := client.CoreRateLimit(ctx)
	if err != nil {
		logger.Warn("failed to read GitHub rate limit; response omits pacing info and the caller will not pause",
			"operation", operation, "error", err)
		return nil
	}
	return &apitypes.GitHubRateLimit{
		Remaining: remaining,
		Limit:     limit,
		ResetAt:   resetAt.UTC().Format(time.RFC3339),
	}
}

func (s *Service) handleChecksScan(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeDirectAdminWrite(w, r, "checks_scan") {
		return
	}
	var req ChecksScanRequest
	if err := decodeStrictJSONBody(r, &req); err != nil {
		s.writeBodyDecodeError(w, err)
		return
	}
	ctx, cancel := s.extendWebhookOpsDeadline(w, r)
	defer cancel()
	response, err := executeChecksScan(ctx, s.config, s.storage, req, s.logger)
	if err != nil {
		s.writeWebhookOpsError(w, "checks_scan", canonicalRepo(req.Repo), err,
			"environment", req.Environment, "check_name", req.CheckName, "page", req.Page, "prs", req.PRs)
		return
	}
	s.writeJSON(w, http.StatusOK, response)
}

// checksScanClient is the GitHub surface a checks scan reads through: the
// repository's open PRs, the Check Runs on their heads, and the installation's
// remaining budget.
type checksScanClient interface {
	namedPullRequestScanClient
	mergeQueueLister
	CoreRateLimit(ctx context.Context) (remaining, limit int, resetAt time.Time, err error)
}

// mergeQueueLister lists the merge groups a repository's merge queues are
// testing.
type mergeQueueLister interface {
	ListMergeQueueHeads(ctx context.Context, repo string) ([]ghclient.MergeQueueHead, error)
}

// checksScanClientResolver returns the GitHub client a scan of repo reads
// through, using app's installation on the repository.
type checksScanClientResolver func(ctx context.Context, app ResolvedGitHubApp, repo string, logger *slog.Logger) (checksScanClient, error)

// installationChecksScanClient reads through app's installation on repo, the
// same one SchemaBot publishes the repository's Check Runs through. It carries
// cfg's schema directory hints, as the webhook's clients do, so resolving
// which environments a head expects checks for succeeds where the webhook's
// would when GitHub truncates the repository tree.
func installationChecksScanClient(cfg *ServerConfig) checksScanClientResolver {
	return func(ctx context.Context, app ResolvedGitHubApp, repo string, logger *slog.Logger) (checksScanClient, error) {
		client, _, err := resolveAppInstallationClient(ctx, app, repo, logger, ghclient.WithConfigDirHints(cfg))
		if err != nil {
			return nil, err
		}
		return client, nil
	}
}

func executeChecksScan(ctx context.Context, cfg *ServerConfig, store storage.Storage, req ChecksScanRequest, logger *slog.Logger) (*ChecksScanResponse, error) {
	return executeChecksScanWith(ctx, cfg, store, req, logger, installationChecksScanClient(cfg))
}

// checksBackfillSupport is every backfill behavior this server implements.
// Each scan response carries it, so a CLI can refuse a run that would rely on
// a behavior an older server lacks.
var checksBackfillSupport = apitypes.ChecksBackfillSupport{NamedPRs: true, DeferSchemaPRs: true, MergeQueue: true}

func executeChecksScanWith(ctx context.Context, cfg *ServerConfig, store storage.Storage, req ChecksScanRequest, logger *slog.Logger, resolve checksScanClientResolver) (*ChecksScanResponse, error) {
	response, err := scanChecks(ctx, cfg, store, req, logger, resolve)
	if err != nil {
		return nil, err
	}
	response.Supports = checksBackfillSupport
	return response, nil
}

func scanChecks(ctx context.Context, cfg *ServerConfig, store storage.Storage, req ChecksScanRequest, logger *slog.Logger, resolve checksScanClientResolver) (*ChecksScanResponse, error) {
	if cfg == nil {
		return nil, fmt.Errorf("server config is nil")
	}
	req.Repo = canonicalRepo(req.Repo)
	if err := requireRepoFullName(req.Repo); err != nil {
		return nil, err
	}
	if req.Page < 0 {
		return nil, webhookOpsRequestErrorf("page must be non-negative")
	}
	if err := validateNamedScanPRs(req); err != nil {
		return nil, err
	}
	req.Environment = canonicalEnvironment(req.Environment)
	if err := requireNarrowableEnvironment(cfg, req.Environment); err != nil {
		return nil, err
	}
	var updatedSince time.Time
	if req.UpdatedSince != "" {
		var err error
		updatedSince, err = time.Parse(time.RFC3339, req.UpdatedSince)
		if err != nil {
			return nil, webhookOpsRequestErrorf("parse updated_since %q as RFC3339: %v", req.UpdatedSince, err)
		}
	}
	var annotateAfter time.Duration
	if req.StuckAfter != "" {
		var err error
		annotateAfter, err = time.ParseDuration(req.StuckAfter)
		if err != nil {
			return nil, webhookOpsRequestErrorf("parse stuck_after %q as a duration: %v", req.StuckAfter, err)
		}
		if annotateAfter < 0 {
			return nil, webhookOpsRequestErrorf("stuck_after must not be negative")
		}
	}
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(os.Stderr, nil))
	}
	// With Check Run publishing turned off, every open PR would trivially be
	// missing a check the server refuses to create — the scan result would be
	// all noise. Report the repo as disabled instead of scanning, as a normal
	// response rather than an error so a fleet sweep keeps going.
	if !cfg.AreChecksEnabled(req.Repo) {
		logger.Info("checks scan skipping repository with checks disabled", "repo", req.Repo)
		return &ChecksScanResponse{Repo: req.Repo, ChecksDisabled: true}, nil
	}
	app, err := cfg.ResolveGitHubAppForRepo(req.Repo)
	if err != nil {
		return nil, err
	}
	installationClient, err := resolve(ctx, app, req.Repo, logger)
	if errors.Is(err, ghclient.ErrAppNotInstalled) && req.ReportAppNotInstalled {
		// GitHub confirmed the App has no installation covering the repo, so
		// there is nothing this App can scan or recreate there. Report the
		// skip as a normal response so a fleet sweep keeps going. A caller
		// that did not ask for the skip gets the error below instead: it
		// would ignore the flag and read the repo as scanned clean. Every
		// other lookup failure leaves coverage unknown and stays an error.
		logger.Warn("checks scan skipping repository: its GitHub App is not installed on it; install the App on the repository or remove the repository from the repos config",
			"operation", "checks_scan", "repo", req.Repo, "environment", req.Environment, "github_app", app.Name, "error", err)
		return &ChecksScanResponse{Repo: req.Repo, AppNotInstalled: true, GitHubApp: app.Name}, nil
	}
	if err != nil {
		return nil, err
	}
	expectedNames := webhookExpectedCheckNames(cfg, req.Repo, req.Environment, req.CheckName)
	scope := checkScanScope{config: cfg, environment: req.Environment, override: req.CheckName}
	var response *ChecksScanResponse
	if len(req.PRs) > 0 {
		response, err = scanNamedPullRequestsForMissingChecks(ctx, installationClient, store, req.Repo, expectedNames, req.PRs, annotateAfter, logger, scope)
	} else {
		response, err = scanWebhookMissingChecks(ctx, installationClient, store, req.Repo, expectedNames, req.Page, updatedSince, annotateAfter, logger, scope)
	}
	if err != nil {
		return nil, err
	}
	if scansMergeQueue(req) {
		// The queue only adds findings, so failing to read it must not stop
		// the PRs' recovery in this repository or the ones after it. The
		// response says the queue went unread, and the caller counts it as
		// needing attention rather than as clear.
		if err := scanMergeQueueForMissingChecks(ctx, cfg, installationClient, req.Repo, scope, response, logger); err != nil {
			logger.Warn("checks scan could not read the merge queue; the PRs on this page are still reported and the queue is reported as unchecked",
				"repo", req.Repo, "environment", req.Environment, "check_name", req.CheckName, "error", err)
			response.MergeQueue = nil
			response.MergeQueueScanned = 0
			response.MergeQueueError = mergeQueueUnreadText
		}
	}
	response.RateLimit = gitHubRateLimitSnapshot(ctx, installationClient, logger, "checks scan page")
	return response, nil
}

func (s *Service) handleChecksRepos(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeDirectAdminWrite(w, r, "checks_repos") {
		return
	}
	response, err := executeChecksRepos(s.config)
	if err != nil {
		s.writeWebhookOpsError(w, "checks_repos", "", err)
		return
	}
	s.writeJSON(w, http.StatusOK, response)
}

// executeChecksRepos lists the repositories declared in the server's repos
// config — the inventory a fleet-wide checks scan iterates. Repositories with
// Check Run publishing turned off are listed separately as disabled: a scan
// of one would report every open PR as missing a check the server refuses to
// create, so callers skip them and tell the operator. A legacy single-App
// config declares no repos, so a fleet sweep cannot know what to scan; the
// operator must name repositories explicitly there.
func executeChecksRepos(cfg *ServerConfig) (*ChecksReposResponse, error) {
	if cfg == nil {
		return nil, fmt.Errorf("server config is nil")
	}
	if len(cfg.Repos) == 0 {
		return nil, webhookOpsRequestErrorf("this server declares no repos config; name a repository explicitly instead of scanning all repos")
	}
	response := &ChecksReposResponse{Repos: make([]string, 0, len(cfg.Repos))}
	for repo := range cfg.Repos {
		if cfg.AreChecksEnabled(repo) {
			response.Repos = append(response.Repos, repo)
		} else {
			response.Disabled = append(response.Disabled, repo)
		}
	}
	sort.Strings(response.Repos)
	sort.Strings(response.Disabled)
	return response, nil
}

// resolveRepoInstallationClient builds an installation-scoped GitHub client
// for the App that serves repo, from the server config's App credentials.
func resolveRepoInstallationClient(ctx context.Context, cfg *ServerConfig, repo string, logger *slog.Logger) (*ghclient.InstallationClient, int64, error) {
	app, err := cfg.ResolveGitHubAppForRepo(repo)
	if err != nil {
		return nil, 0, err
	}
	return resolveAppInstallationClient(ctx, app, repo, logger, ghclient.WithListenOnlyRepos(cfg))
}

// resolveAppInstallationClient builds an installation-scoped GitHub client for
// app's installation on repo. An error wrapping ghclient.ErrAppNotInstalled
// means GitHub confirmed app has no installation covering repo.
func resolveAppInstallationClient(ctx context.Context, app ResolvedGitHubApp, repo string, logger *slog.Logger, opts ...ghclient.ClientOption) (*ghclient.InstallationClient, int64, error) {
	creds, err := app.Config.ResolveCredentials()
	if err != nil {
		return nil, 0, fmt.Errorf("app %q: %w", app.Name, err)
	}
	opts = append([]ghclient.ClientOption{ghclient.WithTrustedCheckAppSlugs(app.Config.TrustedCheckAppSlugs)}, opts...)
	client := ghclient.NewClient(creds.AppID, []byte(creds.PrivateKey), logger, opts...)
	installationID, err := client.InstallationIDForRepo(ctx, repo)
	if err != nil {
		return nil, 0, fmt.Errorf("resolve GitHub App %q installation for %s: %w", app.Name, repo, err)
	}
	installationClient, err := client.ForInstallation(installationID)
	if err != nil {
		return nil, 0, fmt.Errorf("create installation client for %s: %w", repo, err)
	}
	return installationClient, installationID, nil
}

// webhookExpectedCheckName is one Check Run name this deployment publishes,
// together with the environment it reports on. Environment is empty when the
// name is not scoped to one, which is what lets a caller tell "this run
// covers everything" apart from "this run covers production".
type webhookExpectedCheckName struct {
	Name        string
	Environment string
}

func webhookExpectedCheckNames(cfg *ServerConfig, repo, environment, override string) []webhookExpectedCheckName {
	if override != "" {
		// An operator-supplied name carries no environment of its own, so the
		// scope is the one the caller asked for, and nothing when they asked
		// for nothing. Dropping an environment they did name would hand the
		// override every environment's rows, which is the mis-attribution
		// this scoping exists to prevent.
		return []webhookExpectedCheckName{{Name: override, Environment: environment}}
	}
	base := cfg.GitHubCheckNameBaseForRepo(repo)
	if environment != "" {
		return []webhookExpectedCheckName{{Name: fmt.Sprintf("%s (%s)", base, environment), Environment: environment}}
	}
	if len(cfg.AllowedEnvironments) > 0 {
		environments, _ := cfg.CheckEnvironmentsForRepo(context.Background(), nil, repo, "")
		out := make([]webhookExpectedCheckName, 0, len(environments))
		for _, env := range environments {
			out = append(out, webhookExpectedCheckName{Name: fmt.Sprintf("%s (%s)", base, env), Environment: env})
		}
		return out
	}
	return []webhookExpectedCheckName{{Name: base}}
}

func webhookMissingCheckNames(cfg *ServerConfig, repo, environment, override string) []string {
	expected := webhookExpectedCheckNames(cfg, repo, environment, override)
	names := make([]string, 0, len(expected))
	for _, name := range expected {
		names = append(names, name.Name)
	}
	return names
}

type webhookMissingCheckScanClient interface {
	ListOpenPullRequestsPage(ctx context.Context, repo string, page, perPage int) (prs []ghclient.OpenPullRequest, nextPage, lastPage int, err error)
	FindCheckRunByName(ctx context.Context, repo, headSHA, checkName string) (*ghclient.CheckRunResult, []string, error)
}

// namedPullRequestScanClient scans PRs the caller names instead of paging
// the repository's open PRs.
type namedPullRequestScanClient interface {
	webhookMissingCheckScanClient
	FetchOpenPullRequest(ctx context.Context, repo string, number int) (pr ghclient.OpenPullRequest, open bool, err error)
}

// scanObservedNow is the clock a scan page ages its runs against, at the
// precision the page reports it.
//
// The threshold is applied on both sides: here, to decide which runs are worth
// reading stored rows for, and by the caller against this same clock, to decide
// which to render. Judging against a finer instant than is reported would make
// the caller's copy of the clock trail the one the decision was made on, and a
// run inside that fraction of a threshold finer than a second would be
// annotated here and dropped there — losing the annotation on the only surface
// that shows it.
func scanObservedNow() time.Time {
	return time.Now().UTC().Truncate(time.Second)
}

// checkRunAgedForAnnotation reports an uncompleted run's start time as the
// caller will read it, and whether it has been sitting long enough to be worth
// reading the stored rows that explain it.
//
// The two answers come from one value on purpose. The caller applies the same
// threshold to the start time it reads back, to decide what to render, and the
// wire format carries whole seconds — so a run judged here against the
// untruncated instant GitHub reported is measured from a fraction of a second
// later than the caller measures it from. At the boundary that is a run left
// unannotated and rendered anyway, with an empty waiting-on column that means
// no stored row was blocking rather than that none was read.
func checkRunAgedForAnnotation(run *ghclient.CheckRunResult, threshold time.Duration, now time.Time) (startedAt string, sittingLongEnough bool) {
	asSent := run.StartedAt.UTC().Truncate(time.Second)
	return formatCheckRunStartedAt(asSent), checkRunSittingLongEnough(asSent, threshold, now)
}

// checkRunSittingLongEnough reports whether an uncompleted Check Run has been
// sitting at least as long as the caller's threshold.
//
// A start time that is absent or in the future proves nothing about the run's
// age, so it counts as long enough. The threshold exists to skip work the
// caller will not display, and a run whose age cannot be established is one
// the caller displays.
func checkRunSittingLongEnough(startedAt time.Time, threshold time.Duration, now time.Time) bool {
	if threshold <= 0 || startedAt.IsZero() || startedAt.After(now) {
		return true
	}
	return now.Sub(startedAt) >= threshold
}

type checkScanScope struct {
	config      *ServerConfig
	environment string
	override    string
}

func (s checkScanScope) requiresCoverageDiscovery() bool {
	return s.override == "" && len(s.config.AllowedEnvironments) > 0
}

func scanWebhookMissingChecks(ctx context.Context, client webhookMissingCheckScanClient, store storage.Storage, repo string, expectedNames []webhookExpectedCheckName, page int, updatedSince time.Time, annotateAfter time.Duration, logger *slog.Logger, scopes ...checkScanScope) (*ChecksScanResponse, error) {
	prs, nextPage, lastPage, err := client.ListOpenPullRequestsPage(ctx, repo, page, webhookScanPRPageSize)
	if err != nil {
		return nil, err
	}
	now := scanObservedNow()
	result := newChecksScanResponse(repo, expectedNames, now)
	result.NextPage = nextPage
	result.EstimatedOpenPRs = estimateOpenPRCount(page, lastPage, len(prs))
	for i, pr := range prs {
		if !updatedSince.IsZero() && pr.UpdatedAt.Before(updatedSince) {
			// The listing is ordered newest-updated first, so every PR from
			// here on is older than the requested window: the sweep is done.
			prs = prs[:i]
			result.NextPage = 0
			break
		}
	}
	if err := scanPullRequestsForMissingChecks(ctx, client, store, repo, expectedNames, prs, annotateAfter, now, logger, result, scopes...); err != nil {
		return nil, err
	}
	return result, nil
}

// scanNamedPullRequestsForMissingChecks scans only the named PRs, reading
// each one instead of paging the repository, so an operator who already knows
// which PRs to act on pays for those alone. A closed PR is left out, as the
// open-PR listing would leave it out.
func scanNamedPullRequestsForMissingChecks(ctx context.Context, client namedPullRequestScanClient, store storage.Storage, repo string, expectedNames []webhookExpectedCheckName, numbers []int, annotateAfter time.Duration, logger *slog.Logger, scopes ...checkScanScope) (*ChecksScanResponse, error) {
	now := scanObservedNow()
	result := newChecksScanResponse(repo, expectedNames, now)
	prs := make([]ghclient.OpenPullRequest, 0, len(numbers))
	for _, number := range numbers {
		pr, open, err := client.FetchOpenPullRequest(ctx, repo, number)
		if err != nil {
			return nil, fmt.Errorf("read %s#%d to scan it for missing or stuck Check Runs: %w", repo, number, err)
		}
		if !open {
			logger.Info("checks scan leaving out a named PR that is not open", "repo", repo, "pr", number)
			continue
		}
		prs = append(prs, pr)
	}
	result.EstimatedOpenPRs = len(prs)
	if err := scanPullRequestsForMissingChecks(ctx, client, store, repo, expectedNames, prs, annotateAfter, now, logger, result, scopes...); err != nil {
		return nil, err
	}
	return result, nil
}

// checksScanMaxNamedPRs bounds a scan of named PRs to one listing page's
// worth, so it finishes within the same deadline a paged scan does.
const checksScanMaxNamedPRs = webhookScanPRPageSize

// validateNamedScanPRs rejects a named-PR scan that also asks for paging or a
// time window, which only apply to a scan of the repository's open PRs.
func validateNamedScanPRs(req ChecksScanRequest) error {
	if len(req.PRs) == 0 {
		return nil
	}
	if len(req.PRs) > checksScanMaxNamedPRs {
		return webhookOpsRequestErrorf("at most %d prs per scan; send the rest in follow-up requests", checksScanMaxNamedPRs)
	}
	if req.Page > 0 {
		return webhookOpsRequestErrorf("page does not apply to a scan of named prs")
	}
	if req.MergeQueue {
		return webhookOpsRequestErrorf("merge_queue does not apply to a scan of named prs")
	}
	if req.UpdatedSince != "" {
		return webhookOpsRequestErrorf("updated_since does not apply to a scan of named prs")
	}
	for _, pr := range req.PRs {
		if pr <= 0 {
			return webhookOpsRequestErrorf("pr numbers must be positive; got %d", pr)
		}
	}
	return nil
}

// scansMergeQueue reports whether this scan request reads the merge queue: it
// asked to, and this is the first page of a paged scan, so a multi-page sweep
// reads the queue once.
func scansMergeQueue(req ChecksScanRequest) bool {
	return req.MergeQueue && len(req.PRs) == 0 && req.Page <= 1
}

// mergeQueueScanClient is the GitHub surface a merge queue scan reads
// through.
type mergeQueueScanClient interface {
	mergeQueueLister
	FindCheckRunByName(ctx context.Context, repo, headSHA, checkName string) (*ghclient.CheckRunResult, []string, error)
}

// scanMergeQueueForMissingChecks records in result each merge group whose head
// lacks an expected SchemaBot check. Such a group holds every PR queued behind
// it until the queue times it out. A queue that cannot be listed is an error,
// not an empty queue, so a sweep never reports a blocked queue as clear. On an
// error, result may hold some of the queue's groups; the caller discards them.
//
// An aggregate participant's checks are never required on a merge group (the
// leader posts the required one), so its queue is not scanned: every group
// would read as missing a check that is not supposed to exist.
func scanMergeQueueForMissingChecks(ctx context.Context, cfg *ServerConfig, client mergeQueueScanClient, repo string, scope checkScanScope, result *ChecksScanResponse, logger *slog.Logger) error {
	if cfg.AggregateRoleForRepo(repo) == AggregateRoleParticipant {
		logger.Info("checks scan leaving out the merge queue: an aggregate participant posts no merge queue checks; the leader does", "repo", repo)
		return nil
	}
	heads, err := client.ListMergeQueueHeads(ctx, repo)
	if err != nil {
		return fmt.Errorf("scan merge queue of %s for missing checks: %w", repo, err)
	}
	result.MergeQueueScanned = len(heads)
	for _, head := range heads {
		names, err := resolvedWebhookCheckNames(ctx, cfg, client, repo, head.HeadSHA, scope.environment, scope.override, nil)
		if err != nil {
			return fmt.Errorf("resolve expected checks for merge group %s of %s at %s: %w", head.Ref, repo, head.HeadSHA, err)
		}
		var missing, untrustedConflicts []string
		for _, expected := range names {
			run, untrustedApps, err := client.FindCheckRunByName(ctx, repo, head.HeadSHA, expected.Name)
			if err != nil {
				return fmt.Errorf("scan merge group %s of %s for check %q at %s: %w", head.Ref, repo, expected.Name, head.HeadSHA, err)
			}
			if run != nil {
				continue
			}
			missing = append(missing, expected.Name)
			if len(untrustedApps) > 0 {
				untrustedConflicts = append(untrustedConflicts, expected.Name)
			}
		}
		if len(missing) == 0 {
			continue
		}
		entry := MissingMergeQueueCheck{
			Ref:                    head.Ref,
			BaseRef:                head.BaseRef,
			PR:                     head.PR,
			HeadSHA:                head.HeadSHA,
			MissingNames:           missing,
			UntrustedConflictNames: untrustedConflicts,
		}
		if head.PR > 0 {
			entry.URL = caller.PullRequestURL(repo, head.PR)
		}
		logger.Info("checks scan found a merge group missing SchemaBot checks; it blocks the PRs queued behind it",
			"repo", repo, "ref", head.Ref, "pr", head.PR, "head_sha", head.HeadSHA, "missing", missing)
		result.MergeQueue = append(result.MergeQueue, entry)
	}
	return nil
}

// newChecksScanResponse starts a scan response for repo, stamped with the
// clock the scan ages its runs against.
func newChecksScanResponse(repo string, expectedNames []webhookExpectedCheckName, now time.Time) *ChecksScanResponse {
	checkNames := make([]string, 0, len(expectedNames))
	for _, expected := range expectedNames {
		checkNames = append(checkNames, expected.Name)
	}
	return &ChecksScanResponse{
		Repo:       repo,
		CheckNames: checkNames,
		ObservedAt: now.Format(time.RFC3339),
	}
}

// scanPullRequestsForMissingChecks asks of each PR whether its expected Check
// Runs are present and completed, and records the answers in result.
func scanPullRequestsForMissingChecks(ctx context.Context, client webhookMissingCheckScanClient, store storage.Storage, repo string, expectedNames []webhookExpectedCheckName, prs []ghclient.OpenPullRequest, annotateAfter time.Duration, now time.Time, logger *slog.Logger, result *ChecksScanResponse, scopes ...checkScanScope) error {
	for _, pr := range prs {
		result.Scanned++
		var missing []string
		var untrustedConflicts []string
		var incomplete []IncompleteCheckRun
		// The environment each uncompleted run reports on, positionally
		// aligned with incomplete, so its rows can be narrowed to it once the
		// PR's stored state has been read.
		var incompleteEnvironments []string
		// Whether each uncompleted run has been sitting long enough for the
		// caller to render it, positionally aligned with incomplete. A run
		// that has not is not worth annotating, since the caller drops it
		// before it reaches an operator.
		//
		// The decision is per run, not per pull request: a PR can carry an old
		// run beside a young one, and the old one is exactly what the caller
		// is about to render. Letting the young one speak for both would send
		// back the run an operator is looking at with nothing explaining it.
		var annotate []bool
		prNames := expectedNames
		var stored []*storage.Check
		var err error
		storedRead := false
		// Fixed names need no retained coverage. Storage is only an optional
		// source of annotations for their stuck runs.
		if len(scopes) > 0 && scopes[0].requiresCoverageDiscovery() {
			stored, err = store.Checks().GetByPR(ctx, repo, pr.Number)
			if err != nil {
				return fmt.Errorf("read retained check coverage %s#%d: %w", repo, pr.Number, err)
			}
			storedRead = true
			prNames, err = resolvedWebhookCheckNames(ctx, scopes[0].config, client, repo, pr.HeadSHA, scopes[0].environment, scopes[0].override, stored)
			if err != nil {
				return err
			}
		}
		for _, expected := range prNames {
			run, untrustedApps, err := client.FindCheckRunByName(ctx, repo, pr.HeadSHA, expected.Name)
			if err != nil {
				return fmt.Errorf("scan %s#%d for check %q at %s: %w", repo, pr.Number, expected.Name, pr.HeadSHA, err)
			}
			if run != nil {
				if !checkRunCompleted(run) {
					startedAt, sittingLongEnough := checkRunAgedForAnnotation(run, annotateAfter, now)
					incomplete = append(incomplete, IncompleteCheckRun{
						Name:       run.Name,
						CheckRunID: run.ID,
						Status:     run.Status,
						StartedAt:  startedAt,
					})
					incompleteEnvironments = append(incompleteEnvironments, expected.Environment)
					annotate = append(annotate, sittingLongEnough)
				}
				continue
			}
			missing = append(missing, expected.Name)
			// No trusted check exists, but a same-named one from an untrusted
			// app does: backfill will still create the trusted check, yet the
			// operator likely also needs to resolve the conflicting check.
			if len(untrustedApps) > 0 {
				untrustedConflicts = append(untrustedConflicts, expected.Name)
			}
		}
		if len(incomplete) > 0 {
			// One read serves every run on this PR that earned an annotation,
			// so it is made once and only when at least one did; each run then
			// keeps only the rows for the environment it gates.
			if slices.Contains(annotate, true) && !storedRead {
				stored = storedChecksBehindStuckCheck(ctx, store, repo, pr.Number, pr.HeadSHA, logger)
			}
			for i := range incomplete {
				if !annotate[i] {
					logger.Debug("checks scan reporting an uncompleted Check Run without its stored rows: it has not been sitting long enough for the caller to render",
						"repo", repo, "pr", pr.Number, "head_sha", pr.HeadSHA, "check_name", incomplete[i].Name)
					continue
				}
				var checks []*storage.Check
				var scopeConfig *ServerConfig
				if len(scopes) > 0 {
					scopeConfig = scopes[0].config
					checks = scopeConfig.CheckRowsForEnvironment(stored, incompleteEnvironments[i], repo, pr.Number, pr.HeadSHA)
				} else {
					checks = checkRowsForScanEnvironment(stored, incompleteEnvironments[i])
				}
				scoped := inspectRowsBehindStuckCheck(ctx, scopeConfig, store, checks, pr.HeadSHA, logger)
				incomplete[i].StoredRows = scoped
				incomplete[i].WaitingOn = waitingOnForStoredRows(scoped)
			}
			result.Stuck = append(result.Stuck, StuckCheckPR{
				Number:  pr.Number,
				URL:     caller.PullRequestURL(repo, pr.Number),
				Title:   pr.Title,
				HeadSHA: pr.HeadSHA,
				HeadRef: pr.HeadRef,
				Checks:  incomplete,
			})
		}
		if len(missing) == 0 {
			continue
		}
		result.Missing = append(result.Missing, MissingCheckPR{
			Number:                 pr.Number,
			URL:                    caller.PullRequestURL(repo, pr.Number),
			Title:                  pr.Title,
			HeadSHA:                pr.HeadSHA,
			HeadRef:                pr.HeadRef,
			MissingNames:           missing,
			UntrustedConflictNames: untrustedConflicts,
		})
	}
	return nil
}

// estimateOpenPRCount derives the repository's total open-PR count from one
// listing page, so a long scan can report progress against a denominator that
// is recomputed every page instead of feeling unbounded. GitHub reports the
// listing's last page while more pages remain — an upper bound of
// lastPage×pageSize — and on the final page (lastPage 0) the count is exact:
// the PRs before this page plus the PRs on it.
func estimateOpenPRCount(page, lastPage, pageLen int) int {
	if lastPage > 0 {
		return lastPage * webhookScanPRPageSize
	}
	if page <= 0 {
		page = 1
	}
	return (page-1)*webhookScanPRPageSize + pageLen
}

// storedChecksBehindStuckCheck reads the stored check state behind a Check Run
// that exists on an open PR's head and never completed.
//
// The run alone says the gate is open and nothing else: not which database,
// not whether an apply owns it, not whether anything will ever close it. The
// rows say all three, which is what turns a fleet sweep's stuck list from a
// list of PRs to open into a list of PRs to act on.
//
// A read failure costs the entry its rows and nothing else. The Check Run
// findings are what the backfill acts on and they are already in hand, so
// failing the whole scan over the explanation would trade the answer for the
// annotation.
func storedChecksBehindStuckCheck(ctx context.Context, store storage.Storage, repo string, pr int, headSHA string, logger *slog.Logger) []*storage.Check {
	if store == nil {
		logger.Warn("checks scan reports a stuck Check Run without the stored state behind it: storage is not configured",
			"repo", repo, "pr", pr, "head_sha", headSHA)
		return nil
	}
	stored, err := store.Checks().GetByPR(ctx, repo, pr)
	if err != nil {
		logger.Warn("checks scan reports a stuck Check Run without the stored state behind it: reading stored check state failed",
			"repo", repo, "pr", pr, "head_sha", headSHA, "error", err)
		return nil
	}
	if len(stored) == 0 {
		logger.Debug("no stored check state behind this stuck Check Run",
			"repo", repo, "pr", pr, "head_sha", headSHA)
		return nil
	}
	return stored
}

func inspectRowsBehindStuckCheck(ctx context.Context, cfg *ServerConfig, store storage.Storage, checks []*storage.Check, headSHA string, logger *slog.Logger) []InspectedCheck {
	rows := make([]InspectedCheck, 0, len(checks))
	for _, check := range checks {
		rows = append(rows, inspectedCheck(ctx, cfg, store, check, headSHA, logger))
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Environment != rows[j].Environment {
			return rows[i].Environment < rows[j].Environment
		}
		if rows[i].Database != rows[j].Database {
			return rows[i].Database < rows[j].Database
		}
		return rows[i].DatabaseType < rows[j].DatabaseType
	})
	return rows
}

// checkRowsForScanEnvironment narrows a PR's stored rows to the ones a single
// Check Run gates on. An unscoped run keeps all of them: the deployment
// publishes one run covering every environment, so every row is its own.
func checkRowsForScanEnvironment(rows []*storage.Check, environment string) []*storage.Check {
	if environment == "" {
		return rows
	}
	var scoped []*storage.Check
	for _, row := range rows {
		if row.Environment == environment {
			scoped = append(scoped, row)
		}
	}
	return scoped
}

// waitingOnForStoredRows reduces a stuck run's rows to the one answer that
// decides whether an operator opens it. One row needing a person makes the
// whole run need one: a run reported as self-converging when part of it is
// not would be read as safe to leave alone.
//
// No blocking row means no stored row explains the run, which is reported as
// no answer. Calling that "schemabot" would promise a convergence nothing
// recorded is going to deliver, and that is the reading that leaves a wedged
// gate sitting.
//
// The rollup is a projection of the rows beside it and never a cause of its
// own, so it decides nothing here.
func waitingOnForStoredRows(rows []InspectedCheck) string {
	waiting := ""
	for _, row := range rows {
		if row.Aggregate || !row.Blocking {
			continue
		}
		if !row.SelfConverging {
			return apitypes.WaitingOnOperator
		}
		waiting = apitypes.WaitingOnSchemaBot
	}
	return waiting
}

// checkRunCompleted reports whether the Check Run's status is "completed".
// Any other status (queued, in_progress) is still holding its slot without a
// conclusion, which the scan surfaces so an operator can judge whether the
// run is legitimately in flight or wedged.
func checkRunCompleted(run *ghclient.CheckRunResult) bool {
	return run.Status == "completed"
}

// formatCheckRunStartedAt renders the start time for the API response; GitHub
// can omit it, and a zero time would otherwise serialize as a misleading
// year-one timestamp.
func formatCheckRunStartedAt(startedAt time.Time) string {
	if startedAt.IsZero() {
		return ""
	}
	return startedAt.UTC().Format(time.RFC3339)
}
