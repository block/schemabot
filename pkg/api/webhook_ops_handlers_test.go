package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/checkstate"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type fakeCheckRunBackfiller struct {
	calls    []int
	opts     []CheckRunBackfillOptions
	failPR   int
	outcomes map[int]string
	// deferred maps a PR to the databases it changes; without Plan the fake
	// defers it, as the webhook handler defers a PR that changes schema.
	deferred map[int][]string
	// mergeQueueCalls records each merge group head the fake posted on, and
	// failHead names one whose post fails.
	mergeQueueCalls []string
	failHead        string
	// mergeQueueScopes records the scope each merge group post carried.
	mergeQueueScopes []MergeQueueCheckScope
}

func (f *fakeCheckRunBackfiller) BackfillMergeQueueCheck(_ context.Context, _ string, _ int64, headSHA string, scope MergeQueueCheckScope) (string, error) {
	f.mergeQueueCalls = append(f.mergeQueueCalls, headSHA)
	f.mergeQueueScopes = append(f.mergeQueueScopes, scope)
	if headSHA == f.failHead {
		return "", errors.New("create check run: 502 Bad Gateway")
	}
	return "posted passing merge queue check", nil
}

func (f *fakeCheckRunBackfiller) BackfillPRCheckRuns(_ context.Context, _ string, pr int, _ int64, opts CheckRunBackfillOptions) (CheckRunBackfillResult, error) {
	f.calls = append(f.calls, pr)
	f.opts = append(f.opts, opts)
	if pr == f.failPR {
		return CheckRunBackfillResult{Outcome: "config discovery failed"}, errors.New("boom")
	}
	if databases, ok := f.deferred[pr]; ok && !opts.Plan {
		return CheckRunBackfillResult{Outcome: "deferred: PR changes managed schema", Deferred: true, Databases: databases, ChangedFiles: 3}, nil
	}
	if outcome, ok := f.outcomes[pr]; ok {
		return CheckRunBackfillResult{Outcome: outcome}, nil
	}
	return CheckRunBackfillResult{Outcome: "auto-plan started"}, nil
}

// fakeCoreRateLimit serves a sequence of budget readings, one per call, and
// repeats the last once the sequence runs out.
type fakeCoreRateLimit struct {
	remaining []int
	limit     int
	err       error
	calls     int
}

func (f *fakeCoreRateLimit) CoreRateLimit(context.Context) (int, int, time.Time, error) {
	f.calls++
	if f.err != nil {
		return 0, 0, time.Time{}, f.err
	}
	i := min(f.calls-1, len(f.remaining)-1)
	return f.remaining[i], f.limit, time.Now().Add(time.Hour), nil
}

// fakeMergeQueueSynthesizeClient lists a fixed merge queue on top of a budget
// sequence, the GitHub surface a merge queue backfill reads.
type fakeMergeQueueSynthesizeClient struct {
	fakeCoreRateLimit
	queue    []ghclient.MergeQueueHead
	queueErr error
	// prHeads maps a commit to the open PR whose head it is, and prHeadErr
	// fails that lookup.
	prHeads   map[string]int
	prHeadErr error
}

func (f *fakeMergeQueueSynthesizeClient) OpenPullRequestHeadedAt(_ context.Context, _, sha string) (int, error) {
	if f.prHeadErr != nil {
		return 0, f.prHeadErr
	}
	return f.prHeads[sha], nil
}

func (f *fakeMergeQueueSynthesizeClient) ListMergeQueueHeads(context.Context, string) ([]ghclient.MergeQueueHead, error) {
	if f.queueErr != nil {
		return nil, f.queueErr
	}
	return f.queue, nil
}

// A synthesize batch defers each PR that changes managed schema unless the
// request asks for plans, and carries the databases back so the operator can
// see what planning it would cost.
func TestBackfillPRsWithinBudgetDefersSchemaPRsUnlessPlanned(t *testing.T) {
	t.Parallel()

	backfiller := &fakeCheckRunBackfiller{deferred: map[int][]string{2: {"orders", "payments"}}}
	results := backfillPRsWithinBudget(t.Context(), &fakeCoreRateLimit{remaining: []int{5000}, limit: 5000}, backfiller,
		ChecksSynthesizeRequest{Repo: "octo/repo", PRs: []int{1, 2}}, 7, discardLogger())

	require.Len(t, results, 2)
	assert.Equal(t, ChecksSynthesizeResult{PR: 1, Outcome: "auto-plan started"}, results[0])
	assert.Equal(t, ChecksSynthesizeResult{PR: 2, Outcome: "deferred: PR changes managed schema", Deferred: true, Databases: []string{"orders", "payments"}, ChangedFiles: 3}, results[1])
	assert.Equal(t, []CheckRunBackfillOptions{{Plan: false}, {Plan: false}}, backfiller.opts)

	planned := &fakeCheckRunBackfiller{deferred: map[int][]string{2: {"orders"}}}
	results = backfillPRsWithinBudget(t.Context(), &fakeCoreRateLimit{remaining: []int{5000}, limit: 5000}, planned,
		ChecksSynthesizeRequest{Repo: "octo/repo", PRs: []int{2}, Plan: true}, 7, discardLogger())
	require.Len(t, results, 1)
	assert.Equal(t, ChecksSynthesizeResult{PR: 2, Outcome: "auto-plan started"}, results[0])
	assert.Equal(t, []CheckRunBackfillOptions{{Plan: true}}, planned.opts)
}

// The budget floor is checked before every PR, not only between batches:
// once the budget drops below it, the batch stops and the PRs not started
// come back marked RateLimited for the caller to resend after the reset.
func TestBackfillPRsWithinBudgetStopsAtTheFloor(t *testing.T) {
	t.Parallel()

	backfiller := &fakeCheckRunBackfiller{}
	rate := &fakeCoreRateLimit{remaining: []int{3000, 1500, 900}, limit: 5000}
	results := backfillPRsWithinBudget(t.Context(), rate, backfiller,
		ChecksSynthesizeRequest{Repo: "octo/repo", PRs: []int{1, 2, 3, 4}, RateLimitFloorPercent: 20}, 7, discardLogger())

	assert.Equal(t, []int{1, 2}, backfiller.calls)
	require.Len(t, results, 4)
	assert.Equal(t, "auto-plan started", results[1].Outcome)
	for _, result := range results[2:] {
		assert.True(t, result.RateLimited, "PR %d", result.PR)
		assert.Equal(t, "not started: GitHub budget is below 20% (900 of 5000 requests left)", result.Outcome)
		assert.Empty(t, result.Error)
	}
	assert.Equal(t, []int{3, 4}, []int{results[2].PR, results[3].PR})
}

// The floor is pacing, not a safety gate: with no floor the budget is never
// read, and a budget that cannot be read does not stop the batch.
func TestBackfillPRsWithinBudgetWithoutAReadableFloor(t *testing.T) {
	t.Parallel()

	unread := &fakeCoreRateLimit{remaining: []int{0}, limit: 5000}
	backfiller := &fakeCheckRunBackfiller{}
	results := backfillPRsWithinBudget(t.Context(), unread, backfiller, ChecksSynthesizeRequest{Repo: "octo/repo", PRs: []int{1, 2}}, 7, discardLogger())
	assert.Len(t, results, 2)
	assert.Equal(t, 0, unread.calls)

	failing := &fakeCoreRateLimit{err: errors.New("rate limit endpoint unavailable")}
	backfiller = &fakeCheckRunBackfiller{}
	results = backfillPRsWithinBudget(t.Context(), failing, backfiller, ChecksSynthesizeRequest{Repo: "octo/repo", PRs: []int{1, 2}, RateLimitFloorPercent: 20}, 7, discardLogger())
	assert.Equal(t, []int{1, 2}, backfiller.calls)
	for _, result := range results {
		assert.False(t, result.RateLimited)
	}
}

// A PR whose backfill fails reports that it failed with the outcome, and the
// batch keeps going. The cause stays in the server log.
func TestBackfillPRsWithinBudgetReportsFailuresAndContinues(t *testing.T) {
	t.Parallel()

	backfiller := &fakeCheckRunBackfiller{failPR: 1}
	results := backfillPRsWithinBudget(t.Context(), &fakeCoreRateLimit{remaining: []int{5000}, limit: 5000}, backfiller,
		ChecksSynthesizeRequest{Repo: "octo/repo", PRs: []int{1, 2}}, 7, discardLogger())

	require.Len(t, results, 2)
	assert.Equal(t, ChecksSynthesizeResult{PR: 1, Outcome: "config discovery failed", Error: prBackfillFailedText}, results[0])
	assert.Equal(t, ChecksSynthesizeResult{PR: 2, Outcome: "auto-plan started"}, results[1])
}

// Synthesize requests are validated before any GitHub work: request-shaped
// problems are reported as such, and an instance without a webhook runtime
// cannot backfill at all.
func TestExecuteChecksSynthesizeValidation(t *testing.T) {
	t.Parallel()

	cfg := &ServerConfig{}
	backfiller := &fakeCheckRunBackfiller{}

	_, err := executeChecksSynthesize(t.Context(), cfg, backfiller, ChecksSynthesizeRequest{PRs: []int{1}}, discardLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "repo is required")

	_, err = executeChecksSynthesize(t.Context(), cfg, backfiller, ChecksSynthesizeRequest{Repo: "octo/repo"}, discardLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "prs or merge_queue_heads is required")

	tooManyHeads := make([]string, checksSynthesizeMaxMergeQueueHeadsPerRequest+1)
	for i := range tooManyHeads {
		tooManyHeads[i] = fmt.Sprintf("sha%d", i)
	}
	_, err = executeChecksSynthesize(t.Context(), cfg, backfiller, ChecksSynthesizeRequest{Repo: "octo/repo", MergeQueueHeads: tooManyHeads}, discardLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at most 20 merge_queue_heads per request")

	_, err = executeChecksSynthesize(t.Context(), cfg, backfiller, ChecksSynthesizeRequest{Repo: "octo/repo", MergeQueueHeads: []string{"sha1", ""}}, discardLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "merge_queue_heads must not contain an empty commit")

	tooMany := make([]int, checksSynthesizeMaxPRsPerRequest+1)
	_, err = executeChecksSynthesize(t.Context(), cfg, backfiller, ChecksSynthesizeRequest{Repo: "octo/repo", PRs: tooMany}, discardLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "per request")

	_, err = executeChecksSynthesize(t.Context(), cfg, backfiller, ChecksSynthesizeRequest{Repo: "octo/repo", PRs: []int{1, 0, 2}}, discardLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pr numbers must be positive")

	_, err = executeChecksSynthesize(t.Context(), cfg, nil, ChecksSynthesizeRequest{Repo: "octo/repo", PRs: []int{1}}, discardLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no GitHub webhook runtime")

	narrowable := &ServerConfig{AllowedEnvironments: []string{"staging"}}
	_, err = executeChecksSynthesize(t.Context(), narrowable, backfiller, ChecksSynthesizeRequest{Repo: "octo/repo", MergeQueueHeads: []string{"sha1"}, Environment: "production"}, discardLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), `environment "production" is not one this instance handles`)
	assert.Empty(t, backfiller.mergeQueueScopes, "a refused environment posts nothing")
}

// An operator who types "-e Production" names the environment the scan
// already accepted under that spelling. The synthesize request folds it the
// same way, so the merge queue post is not refused and its scope matches the
// configured lowercase name.
func TestChecksSynthesizeFoldsTheEnvironmentLikeTheScan(t *testing.T) {
	t.Parallel()

	cfg := &ServerConfig{AllowedEnvironments: []string{"staging", "production"}}
	req := ChecksSynthesizeRequest{Repo: "octo/repo", MergeQueueHeads: []string{"sha1"}, Environment: " Production "}
	require.NoError(t, validateChecksSynthesizeRequest(cfg, &req))
	assert.Equal(t, "production", req.Environment)
}

// A repository that is not an owner/name pair is the caller's mistake, and it
// reads as one on every endpoint that takes a repository. Left to the
// installation lookup it would surface as a server error, so the same typo
// would be a 400 on one endpoint and a 500 on the next.
func TestRepoTakingEndpointsRefuseAMalformedRepository(t *testing.T) {
	t.Parallel()

	cfg := &ServerConfig{}
	endpoints := []struct {
		name string
		call func(ctx context.Context, repo string) error
	}{
		{"inspect", func(ctx context.Context, repo string) error {
			store := &inspectStorage{checks: &inspectCheckStore{}, applies: &inspectApplyStore{}}
			_, err := executeChecksInspect(ctx, cfg, store, ChecksInspectRequest{Repo: repo, PullRequest: 7}, discardLogger())
			return err
		}},
		{"scan", func(ctx context.Context, repo string) error {
			_, err := executeChecksScan(ctx, cfg, nil, ChecksScanRequest{Repo: repo}, discardLogger())
			return err
		}},
		{"synthesize", func(ctx context.Context, repo string) error {
			_, err := executeChecksSynthesize(ctx, cfg, &fakeCheckRunBackfiller{}, ChecksSynthesizeRequest{Repo: repo, PRs: []int{1}}, discardLogger())
			return err
		}},
	}

	for _, endpoint := range endpoints {
		t.Run(endpoint.name, func(t *testing.T) {
			t.Parallel()

			err := endpoint.call(t.Context(), "acme")
			require.Error(t, err)
			var requestErr *webhookOpsRequestError
			require.ErrorAs(t, err, &requestErr, "a malformed repository is a 400, not a 500")
			assert.Contains(t, err.Error(), "owner/name pair")
		})
	}
}

// A stale or mistyped environment is rejected as a request error before any
// GitHub work, rather than scanning for a check name that can never exist and
// reporting every PR as missing it.
func TestExecuteChecksScanRejectsDisallowedEnvironment(t *testing.T) {
	t.Parallel()

	cfg := &ServerConfig{AllowedEnvironments: []string{"staging", "production"}}

	_, err := executeChecksScan(t.Context(), cfg, nil, ChecksScanRequest{Repo: "octo/repo", Environment: "prod"}, discardLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), `environment "prod" is not one this instance handles`)
}

// An operator investigating one pull request reaches for the inspection and the
// backfill scan in the same sitting, and types the environment the same way
// into both. Configured names are lowercase, so the two commands agree on what
// "Production" means rather than one answering and the other refusing.
func TestChecksEndpointsAcceptAnEnvironmentHoweverItIsSpelled(t *testing.T) {
	t.Parallel()

	cfg := &ServerConfig{AllowedEnvironments: []string{"staging", "production"}}

	for _, endpoint := range checksEnvironmentTakingEndpoints(t, cfg) {
		t.Run(endpoint.name, func(t *testing.T) {
			t.Parallel()

			// Both get as far as needing a GitHub client they do not have, so
			// what is being pinned is the refusal that does not happen.
			err := endpoint.call(t.Context(), " Production ")
			require.Error(t, err)
			assert.NotContains(t, err.Error(), "is not one this instance handles")
		})
	}
}

// An instance with no configured environments publishes one check that is not
// environment-scoped, so there is no environment to narrow by. Both commands
// say so rather than searching for a check name that instance never creates —
// which would report every open pull request as missing it, and on a sweep that
// acts, re-plan all of them.
func TestChecksEndpointsRefuseAnEnvironmentWhenTheInstanceScopesNoneOfItsChecks(t *testing.T) {
	t.Parallel()

	cfg := &ServerConfig{}

	for _, endpoint := range checksEnvironmentTakingEndpoints(t, cfg) {
		t.Run(endpoint.name, func(t *testing.T) {
			t.Parallel()

			err := endpoint.call(t.Context(), "production")
			require.Error(t, err)
			assert.Contains(t, err.Error(), "there is no environment to narrow to")
		})
	}
}

type checksEnvironmentEndpoint struct {
	name string
	call func(ctx context.Context, environment string) error
}

// checksEnvironmentTakingEndpoints is every checks endpoint an operator can
// hand an environment to, so a rule about environments is stated once over all
// of them instead of once per endpoint — which is how the two came to disagree.
func checksEnvironmentTakingEndpoints(t *testing.T, cfg *ServerConfig) []checksEnvironmentEndpoint {
	t.Helper()
	return []checksEnvironmentEndpoint{
		{"scan", func(ctx context.Context, environment string) error {
			_, err := executeChecksScan(ctx, cfg, nil, ChecksScanRequest{Repo: "octo/repo", Environment: environment}, discardLogger())
			return err
		}},
		{"inspect", func(ctx context.Context, environment string) error {
			req, err := checksInspectRequestFromQuery(url.Values{
				"repo":         {"octo/repo"},
				"pull_request": {"412"},
				"environment":  {environment},
			})
			require.NoError(t, err)
			store := &inspectStorage{checks: &inspectCheckStore{}, applies: &inspectApplyStore{}}
			_, err = executeChecksInspect(ctx, cfg, store, req, discardLogger())
			return err
		}},
	}
}

// productionCheckName is the single environment-scoped Check Run name most
// scan tests expect, so a stuck run's stored rows are read against the one
// environment it gates.
var productionCheckName = []webhookExpectedCheckName{{Name: "SchemaBot (production)", Environment: "production"}}

type fakeWebhookMissingCheckScanClient struct {
	prs  []ghclient.OpenPullRequest
	runs map[string]*ghclient.CheckRunResult
	// untrusted maps headSHA+"/"+checkName to app slugs that published a
	// same-named check but are not trusted.
	untrusted map[string][]string
	// queue is the merge queue the repository lists, and queueErr fails the
	// listing.
	queue    []ghclient.MergeQueueHead
	queueErr error
}

func (f fakeWebhookMissingCheckScanClient) ListMergeQueueHeads(context.Context, string) ([]ghclient.MergeQueueHead, error) {
	if f.queueErr != nil {
		return nil, f.queueErr
	}
	return f.queue, nil
}

func (f fakeWebhookMissingCheckScanClient) ListOpenPullRequestsPage(_ context.Context, _ string, page, perPage int) ([]ghclient.OpenPullRequest, int, int, error) {
	if page <= 0 {
		page = 1
	}
	start := (page - 1) * perPage
	if start >= len(f.prs) {
		return nil, 0, 0, nil
	}
	end := min(start+perPage, len(f.prs))
	nextPage := 0
	lastPage := 0
	if end < len(f.prs) {
		nextPage = page + 1
		// GitHub's Link header names the last page only while more pages
		// remain; the final page carries no last rel.
		lastPage = (len(f.prs) + perPage - 1) / perPage
	}
	return f.prs[start:end], nextPage, lastPage, nil
}

// FetchOpenPullRequest serves a named PR from prs; a PR not among them reads
// as closed, the way a named PR that is no longer open does.
func (f fakeWebhookMissingCheckScanClient) FetchOpenPullRequest(_ context.Context, _ string, number int) (ghclient.OpenPullRequest, bool, error) {
	for _, pr := range f.prs {
		if pr.Number == number {
			return pr, true, nil
		}
	}
	return ghclient.OpenPullRequest{}, false, nil
}

func (f fakeWebhookMissingCheckScanClient) FindCheckRunByName(_ context.Context, _ string, headSHA, checkName string) (*ghclient.CheckRunResult, []string, error) {
	return f.runs[headSHA+"/"+checkName], f.untrusted[headSHA+"/"+checkName], nil
}

func TestWebhookMissingCheckNamesUsesConfiguredEnvironmentNames(t *testing.T) {
	t.Parallel()

	cfg := &ServerConfig{
		GitHub:              GitHubConfig{CheckName: "SchemaBot X"},
		AllowedEnvironments: []string{"staging", "production"},
	}

	assert.Equal(t, []string{"SchemaBot X (staging)", "SchemaBot X (production)"}, webhookMissingCheckNames(cfg, "octo/repo", "", ""))
	assert.Equal(t, []string{"SchemaBot X (production)"}, webhookMissingCheckNames(cfg, "octo/repo", "production", ""))
	assert.Equal(t, []string{"Custom Check"}, webhookMissingCheckNames(cfg, "octo/repo", "production", "Custom Check"))
}

func TestScanWebhookMissingChecksReportsOpenPRsMissingConfiguredChecks(t *testing.T) {
	t.Parallel()

	client := fakeWebhookMissingCheckScanClient{
		prs: []ghclient.OpenPullRequest{
			{Number: 1, Title: "has check", HeadSHA: "sha1", HeadRef: "feature-1"},
			{Number: 2, Title: "missing check", HeadSHA: "sha2", HeadRef: "feature-2"},
		},
		runs: map[string]*ghclient.CheckRunResult{
			"sha1/SchemaBot (production)": {ID: 10, Name: "SchemaBot (production)", Status: "completed", Conclusion: "success"},
		},
	}

	result, err := scanWebhookMissingChecks(t.Context(), client, nil, "octo/repo", productionCheckName, 0, time.Time{}, 0, discardLogger())

	require.NoError(t, err)
	assert.Equal(t, 2, result.Scanned)
	require.Len(t, result.Missing, 1)
	assert.Equal(t, 2, result.Missing[0].Number)
	assert.Equal(t, "https://github.com/octo/repo/pull/2", result.Missing[0].URL)
	assert.Equal(t, []string{"SchemaBot (production)"}, result.Missing[0].MissingNames)
	assert.Empty(t, result.Missing[0].UntrustedConflictNames)
	assert.Equal(t, 2, result.EstimatedOpenPRs, "a single-page listing pins the exact open-PR count")
}

// A scan page carries the repository's open-PR count so the caller can render
// a progress denominator: GitHub's last-page pointer gives an upper bound
// while pages remain, and the final page pins the exact count.
func TestEstimateOpenPRCount(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 50*webhookScanPRPageSize, estimateOpenPRCount(2, 50, webhookScanPRPageSize), "mid-listing: upper bound from GitHub's last-page pointer")
	assert.Equal(t, 3*webhookScanPRPageSize+12, estimateOpenPRCount(4, 0, 12), "final page: the pages before it plus the PRs on it")
	assert.Equal(t, 12, estimateOpenPRCount(0, 0, 12), "an unpaginated listing is the whole repository")
}

// An expected Check Run that exists but never completed is reported as stuck,
// with its raw status and start time, so the operator can tell a wedged check
// apart from a missing one — completed runs and missing runs stay out of the
// stuck list, and an uncompleted run is never misreported as missing.
func TestScanWebhookMissingChecksReportsUncompletedRuns(t *testing.T) {
	t.Parallel()

	startedAt := time.Date(2026, 7, 12, 9, 0, 0, 0, time.UTC)
	client := fakeWebhookMissingCheckScanClient{
		prs: []ghclient.OpenPullRequest{
			{Number: 7, Title: "wedged check", HeadSHA: "sha7", HeadRef: "feature-7"},
			{Number: 8, Title: "healthy check", HeadSHA: "sha8", HeadRef: "feature-8"},
		},
		runs: map[string]*ghclient.CheckRunResult{
			"sha7/SchemaBot (production)": {ID: 70, Name: "SchemaBot (production)", Status: "in_progress", StartedAt: startedAt},
			"sha8/SchemaBot (production)": {ID: 80, Name: "SchemaBot (production)", Status: "completed", Conclusion: "success"},
		},
	}

	result, err := scanWebhookMissingChecks(t.Context(), client, nil, "octo/repo", productionCheckName, 0, time.Time{}, 0, discardLogger())

	require.NoError(t, err)
	assert.Empty(t, result.Missing, "an existing run is not missing, even when uncompleted")
	require.Len(t, result.Stuck, 1)
	stuck := result.Stuck[0]
	assert.Equal(t, 7, stuck.Number)
	assert.Equal(t, "https://github.com/octo/repo/pull/7", stuck.URL)
	require.Len(t, stuck.Checks, 1)
	assert.Equal(t, "SchemaBot (production)", stuck.Checks[0].Name)
	assert.Equal(t, int64(70), stuck.Checks[0].CheckRunID)
	assert.Equal(t, "in_progress", stuck.Checks[0].Status)
	assert.Equal(t, "2026-07-12T09:00:00Z", stuck.Checks[0].StartedAt)
}

// A stuck run without a reported start time serializes an empty started_at
// rather than a misleading year-one timestamp.
func TestScanWebhookMissingChecksReportsUncompletedRunWithoutStartTime(t *testing.T) {
	t.Parallel()

	client := fakeWebhookMissingCheckScanClient{
		prs: []ghclient.OpenPullRequest{
			{Number: 9, Title: "queued forever", HeadSHA: "sha9", HeadRef: "feature-9"},
		},
		runs: map[string]*ghclient.CheckRunResult{
			"sha9/SchemaBot (production)": {ID: 90, Name: "SchemaBot (production)", Status: "queued"},
		},
	}

	result, err := scanWebhookMissingChecks(t.Context(), client, nil, "octo/repo", productionCheckName, 0, time.Time{}, 0, discardLogger())

	require.NoError(t, err)
	assert.Empty(t, result.Missing)
	require.Len(t, result.Stuck, 1)
	require.Len(t, result.Stuck[0].Checks, 1)
	assert.Equal(t, "queued", result.Stuck[0].Checks[0].Status)
	assert.Empty(t, result.Stuck[0].Checks[0].StartedAt)
}

// A windowed sweep stops at the window boundary: the open-PR listing is
// ordered newest-updated first, so once a PR older than updated_since
// appears the rest of the repo cannot be in the window — the scan reports
// only the in-window PRs and clears next_page so the caller stops paging.
// This bounds an incident sweep by the incident window, not the repo's total
// open-PR count.
func TestScanWebhookMissingChecksStopsAtUpdatedSince(t *testing.T) {
	t.Parallel()

	updatedSince := time.Date(2026, 7, 12, 8, 0, 0, 0, time.UTC)
	client := fakeWebhookMissingCheckScanClient{
		prs: []ghclient.OpenPullRequest{
			{Number: 1, Title: "in window, missing check", HeadSHA: "sha1", HeadRef: "f1", UpdatedAt: updatedSince.Add(2 * time.Hour)},
			{Number: 2, Title: "in window, has check", HeadSHA: "sha2", HeadRef: "f2", UpdatedAt: updatedSince.Add(time.Hour)},
			{Number: 3, Title: "before window, also missing check", HeadSHA: "sha3", HeadRef: "f3", UpdatedAt: updatedSince.Add(-time.Hour)},
			{Number: 4, Title: "before window", HeadSHA: "sha4", HeadRef: "f4", UpdatedAt: updatedSince.Add(-2 * time.Hour)},
		},
		runs: map[string]*ghclient.CheckRunResult{
			"sha2/SchemaBot (production)": {ID: 20, Name: "SchemaBot (production)", Status: "completed", Conclusion: "success"},
		},
	}

	result, err := scanWebhookMissingChecks(t.Context(), client, nil, "octo/repo", productionCheckName, 0, updatedSince, 0, discardLogger())

	require.NoError(t, err)
	assert.Equal(t, 2, result.Scanned, "only the in-window PRs count as scanned")
	assert.Zero(t, result.NextPage, "crossing the window boundary ends the sweep")
	require.Len(t, result.Missing, 1)
	assert.Equal(t, 1, result.Missing[0].Number)
}

// An unparseable updated_since is a request error, not a full unwindowed scan.
func TestExecuteChecksScanRejectsInvalidUpdatedSince(t *testing.T) {
	t.Parallel()

	cfg := &ServerConfig{AllowedEnvironments: []string{"production"}}

	_, err := executeChecksScan(t.Context(), cfg, nil, ChecksScanRequest{Repo: "octo/repo", UpdatedSince: "yesterday"}, discardLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "updated_since")
}

// The repos inventory splits the declared repos config into scannable repos
// and repos with Check Run publishing turned off, both sorted — a fleet sweep
// scans the former and reports the latter as skipped. A legacy single-App
// config (which declares no repos) is a request error telling the operator to
// name a repository — a fleet sweep cannot guess what to scan.
func TestExecuteChecksRepos(t *testing.T) {
	t.Parallel()

	checksOff := false
	cfg := &ServerConfig{
		Apps: map[string]GitHubAppConfig{"main": {AppID: "1", PrivateKey: "key"}},
		Repos: map[string]RepoConfig{
			"octo/zebra":    {GitHubApp: "main"},
			"octo/alpha":    {GitHubApp: "main"},
			"octo/no-check": {GitHubApp: "main", EnableChecks: &checksOff},
		},
	}
	response, err := executeChecksRepos(cfg)
	require.NoError(t, err)
	assert.Equal(t, []string{"octo/alpha", "octo/zebra"}, response.Repos)
	assert.Equal(t, []string{"octo/no-check"}, response.Disabled)

	_, err = executeChecksRepos(&ServerConfig{GitHub: GitHubConfig{AppID: "1", PrivateKey: "key"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "declares no repos config")
}

// A repository whose Check Run publishing is turned off ends up entirely in
// the disabled list; the response says explicitly that nothing is scannable
// instead of erroring, so a fleet sweep reports the skips and finishes clean.
func TestExecuteChecksReposAllDisabled(t *testing.T) {
	t.Parallel()

	checksOff := false
	cfg := &ServerConfig{
		Apps: map[string]GitHubAppConfig{"main": {AppID: "1", PrivateKey: "key"}},
		Repos: map[string]RepoConfig{
			"octo/one": {GitHubApp: "main", EnableChecks: &checksOff},
			"octo/two": {GitHubApp: "main", EnableChecks: &checksOff},
		},
	}
	response, err := executeChecksRepos(cfg)
	require.NoError(t, err)
	assert.Empty(t, response.Repos)
	assert.Equal(t, []string{"octo/one", "octo/two"}, response.Disabled)
}

// Synthesize against a repository with Check Run publishing turned off skips
// every requested PR without touching GitHub or the auto-plan flow: the
// publisher would refuse the checks anyway, and replaying auto-plans would
// leave plan comments as the only side effect. A skip outcome, not an error,
// so a fleet sweep that includes a disabled repo keeps going.
func TestExecuteChecksSynthesizeSkipsRepoWithChecksDisabled(t *testing.T) {
	t.Parallel()

	checksOff := false
	cfg := &ServerConfig{
		Apps:  map[string]GitHubAppConfig{"main": {AppID: "1", PrivateKey: "key"}},
		Repos: map[string]RepoConfig{"octo/repo": {GitHubApp: "main", EnableChecks: &checksOff}},
	}
	backfiller := &fakeCheckRunBackfiller{}

	response, err := executeChecksSynthesize(t.Context(), cfg, backfiller, ChecksSynthesizeRequest{Repo: "octo/repo", PRs: []int{1, 2}}, discardLogger())
	require.NoError(t, err)
	assert.Empty(t, backfiller.calls)
	require.Len(t, response.Results, 2)
	for i, pr := range []int{1, 2} {
		assert.Equal(t, pr, response.Results[i].PR)
		assert.Equal(t, checksDisabledSkipOutcome, response.Results[i].Outcome)
		assert.Empty(t, response.Results[i].Error)
	}
}

// Scanning a repository with Check Run publishing turned off reports the repo
// as disabled instead of scanning: every open PR would trivially be missing a
// check the server refuses to create. A normal response, not an error, so a
// fleet sweep that includes a disabled repo keeps going.
func TestExecuteChecksScanSkipsRepoWithChecksDisabled(t *testing.T) {
	t.Parallel()

	checksOff := false
	cfg := &ServerConfig{
		Apps:  map[string]GitHubAppConfig{"main": {AppID: "1", PrivateKey: "key"}},
		Repos: map[string]RepoConfig{"octo/repo": {GitHubApp: "main", EnableChecks: &checksOff}},
	}

	response, err := executeChecksScan(t.Context(), cfg, nil, ChecksScanRequest{Repo: "octo/repo"}, discardLogger())
	require.NoError(t, err)
	assert.True(t, response.ChecksDisabled)
	assert.Equal(t, "octo/repo", response.Repo)
	assert.Zero(t, response.Scanned)
	assert.Empty(t, response.Missing)
	assert.Equal(t, apitypes.ChecksBackfillSupport{NamedPRs: true, DeferSchemaPRs: true, MergeQueue: true}, response.Supports, "a skipped repository still tells the CLI what this server supports")
}

// fakeChecksScanClient adds the installation budget read a scan page ends
// with to the open-PR and Check Run reads the scan itself makes.
type fakeChecksScanClient struct {
	fakeWebhookMissingCheckScanClient
}

func (fakeChecksScanClient) CoreRateLimit(context.Context) (int, int, time.Time, error) {
	return 4000, 5000, time.Now().Add(time.Hour), nil
}

// checksScanResolverByRepo serves each repository's scan from clients, and
// fails the installation lookup for any repository in lookupErrs with that
// error, the way the production resolver surfaces a failed lookup.
func checksScanResolverByRepo(clients map[string]checksScanClient, lookupErrs map[string]error) checksScanClientResolver {
	return func(_ context.Context, app ResolvedGitHubApp, repo string, _ *slog.Logger) (checksScanClient, error) {
		if err, ok := lookupErrs[repo]; ok {
			return nil, fmt.Errorf("resolve GitHub App %q installation for %s: %w", app.Name, repo, err)
		}
		client, ok := clients[repo]
		if !ok {
			return nil, fmt.Errorf("no fake scan client for %s", repo)
		}
		return client, nil
	}
}

// A repository declared in the repos config before its GitHub App is installed
// on it, for example while its onboarding PR is still open, is reported as
// skipped with the App's name rather than failing the scan. A fleet sweep that
// reaches it keeps going, and the server logs the skip at Warn with what an
// operator needs to fix it.
func TestExecuteChecksScanSkipsRepoWithoutTheAppInstalled(t *testing.T) {
	t.Parallel()

	cfg := &ServerConfig{
		Apps:                map[string]GitHubAppConfig{"main": {AppID: "1", PrivateKey: "key"}},
		Repos:               map[string]RepoConfig{"octo/onboarding": {GitHubApp: "main"}},
		AllowedEnvironments: []string{"staging", "production"},
	}
	notInstalled := fmt.Errorf("find installation for octo/onboarding: %w: GET https://api.github.com/repos/octo/onboarding/installation: 404 Not Found []", ghclient.ErrAppNotInstalled)
	resolve := checksScanResolverByRepo(nil, map[string]error{"octo/onboarding": notInstalled})
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo}))

	response, err := executeChecksScanWith(t.Context(), cfg, nil, ChecksScanRequest{Repo: "octo/onboarding", Environment: "staging", ReportAppNotInstalled: true}, logger, resolve)
	require.NoError(t, err)
	assert.True(t, response.AppNotInstalled)
	assert.Equal(t, "main", response.GitHubApp)
	assert.Equal(t, "octo/onboarding", response.Repo)
	assert.False(t, response.ChecksDisabled)
	assert.Zero(t, response.Scanned)
	assert.Empty(t, response.Missing)
	assert.Empty(t, response.CheckNames)
	assert.Equal(t, apitypes.ChecksBackfillSupport{NamedPRs: true, DeferSchemaPRs: true, MergeQueue: true}, response.Supports, "a skipped repository still tells the CLI what this server supports")

	var entry map[string]any
	require.NoError(t, json.Unmarshal(logs.Bytes(), &entry), "exactly one log line: %s", logs.String())
	assert.Equal(t, "WARN", entry["level"])
	assert.Contains(t, entry["msg"], "GitHub App is not installed")
	assert.Equal(t, "checks_scan", entry["operation"])
	assert.Equal(t, "octo/onboarding", entry["repo"])
	assert.Equal(t, "staging", entry["environment"])
	assert.Equal(t, "main", entry["github_app"])
	assert.Contains(t, entry["error"], "404 Not Found")
}

// A caller that does not say it handles the not-installed skip predates it,
// and would ignore the flag and read the unread repository as scanned clean.
// It gets the lookup error instead, naming the App and the repository.
func TestExecuteChecksScanFailsForACallerThatDoesNotReportTheSkip(t *testing.T) {
	t.Parallel()

	cfg := &ServerConfig{
		Apps:  map[string]GitHubAppConfig{"main": {AppID: "1", PrivateKey: "key"}},
		Repos: map[string]RepoConfig{"octo/onboarding": {GitHubApp: "main"}},
	}
	notInstalled := fmt.Errorf("find installation for octo/onboarding: %w: GET https://api.github.com/repos/octo/onboarding/installation: 404 Not Found []", ghclient.ErrAppNotInstalled)
	resolve := checksScanResolverByRepo(nil, map[string]error{"octo/onboarding": notInstalled})

	response, err := executeChecksScanWith(t.Context(), cfg, nil, ChecksScanRequest{Repo: "octo/onboarding"}, discardLogger(), resolve)
	require.Error(t, err)
	assert.Nil(t, response)
	require.ErrorIs(t, err, ghclient.ErrAppNotInstalled)
	assert.Contains(t, err.Error(), `resolve GitHub App "main" installation for octo/onboarding`)
	assert.Contains(t, err.Error(), "GitHub App is not installed on the repository")
}

// Any installation lookup failure other than GitHub confirming the App is not
// installed leaves the App's coverage of the repository unknown. A scan that
// skipped it would let a fleet sweep report the repository as having nothing
// to backfill when it was never looked at, so the failure stays an error.
func TestExecuteChecksScanFailsWhenTheInstallationLookupFails(t *testing.T) {
	t.Parallel()

	cfg := &ServerConfig{
		Apps:  map[string]GitHubAppConfig{"main": {AppID: "1", PrivateKey: "key"}},
		Repos: map[string]RepoConfig{"octo/repo": {GitHubApp: "main"}},
	}
	outage := errors.New("find installation for octo/repo: GET https://api.github.com/repos/octo/repo/installation: 502 Bad Gateway []")
	resolve := checksScanResolverByRepo(nil, map[string]error{"octo/repo": outage})

	response, err := executeChecksScanWith(t.Context(), cfg, nil, ChecksScanRequest{Repo: "octo/repo"}, discardLogger(), resolve)
	require.Error(t, err)
	assert.Nil(t, response)
	require.ErrorIs(t, err, outage)
	assert.NotErrorIs(t, err, ghclient.ErrAppNotInstalled)
	assert.Contains(t, err.Error(), "502 Bad Gateway")
}

// A repository the App is installed on scans through the resolved
// installation client and reports what it found.
func TestExecuteChecksScanScansThroughTheResolvedInstallation(t *testing.T) {
	t.Parallel()

	cfg := &ServerConfig{
		Apps:  map[string]GitHubAppConfig{"main": {AppID: "1", PrivateKey: "key"}},
		Repos: map[string]RepoConfig{"octo/repo": {GitHubApp: "main"}},
	}
	resolve := checksScanResolverByRepo(map[string]checksScanClient{
		"octo/repo": fakeChecksScanClient{fakeWebhookMissingCheckScanClient{
			prs: []ghclient.OpenPullRequest{{Number: 7, Title: "add column", HeadSHA: "sha7", HeadRef: "feature-7"}},
		}},
	}, nil)

	store := &inspectStorage{checks: &inspectCheckStore{}}
	response, err := executeChecksScanWith(t.Context(), cfg, store, ChecksScanRequest{Repo: "octo/repo"}, discardLogger(), resolve)
	require.NoError(t, err)
	assert.False(t, response.AppNotInstalled)
	assert.Empty(t, response.GitHubApp)
	assert.Equal(t, apitypes.ChecksBackfillSupport{NamedPRs: true, DeferSchemaPRs: true, MergeQueue: true}, response.Supports)
	assert.Equal(t, 1, response.Scanned)
	require.Len(t, response.Missing, 1)
	assert.Equal(t, 7, response.Missing[0].Number)
	require.NotNil(t, response.RateLimit)
	assert.Equal(t, 4000, response.RateLimit.Remaining)
}

func TestExecuteChecksScanFixedNamesSurviveStorageOutage(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name         string
		environments []string
		override     string
		checkName    string
	}{
		{name: "unscoped", checkName: "SchemaBot"},
		{name: "explicit override", environments: []string{"production"}, override: "Custom Gate", checkName: "Custom Gate"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := &ServerConfig{
				Apps:                map[string]GitHubAppConfig{"main": {AppID: "1", PrivateKey: "key"}},
				Repos:               map[string]RepoConfig{"octo/repo": {GitHubApp: "main"}},
				AllowedEnvironments: tc.environments,
			}
			resolve := checksScanResolverByRepo(map[string]checksScanClient{
				"octo/repo": fakeChecksScanClient{fakeWebhookMissingCheckScanClient{
					prs: []ghclient.OpenPullRequest{
						{Number: 7, HeadSHA: "sha7"},
						{Number: 8, HeadSHA: "sha8"},
					},
					runs: map[string]*ghclient.CheckRunResult{
						"sha8/" + tc.checkName: {ID: 80, Name: tc.checkName, Status: "in_progress"},
					},
				}},
			}, nil)
			checks := &inspectCheckStore{err: errors.New("storage unavailable")}
			store := &inspectStorage{checks: checks}

			response, err := executeChecksScanWith(t.Context(), cfg, store, ChecksScanRequest{Repo: "octo/repo", CheckName: tc.override}, discardLogger(), resolve)

			require.NoError(t, err)
			assert.Equal(t, 2, response.Scanned)
			require.Len(t, response.Missing, 1)
			assert.Equal(t, 7, response.Missing[0].Number)
			assert.Equal(t, []string{tc.checkName}, response.Missing[0].MissingNames)
			require.Len(t, response.Stuck, 1)
			assert.Equal(t, 8, response.Stuck[0].Number)
			require.Len(t, response.Stuck[0].Checks, 1)
			assert.Equal(t, tc.checkName, response.Stuck[0].Checks[0].Name)
			assert.Empty(t, response.Stuck[0].Checks[0].StoredRows)
			assert.Empty(t, response.Stuck[0].Checks[0].WaitingOn)
			assert.Equal(t, 1, checks.reads, "only stuck-run annotation needs storage")
		})
	}
}

func TestExecuteChecksScanRequiresRetainedEnvironmentCoverage(t *testing.T) {
	t.Parallel()

	cfg := &ServerConfig{
		Apps:                map[string]GitHubAppConfig{"main": {AppID: "1", PrivateKey: "key"}},
		Repos:               map[string]RepoConfig{"octo/repo": {GitHubApp: "main"}},
		AllowedEnvironments: []string{"production"},
	}
	resolve := checksScanResolverByRepo(map[string]checksScanClient{
		"octo/repo": fakeChecksScanClient{fakeWebhookMissingCheckScanClient{
			prs: []ghclient.OpenPullRequest{{Number: 7, HeadSHA: "sha7"}},
		}},
	}, nil)
	outage := errors.New("storage unavailable")
	store := &inspectStorage{checks: &inspectCheckStore{err: outage}}

	response, err := executeChecksScanWith(t.Context(), cfg, store, ChecksScanRequest{Repo: "octo/repo"}, discardLogger(), resolve)

	require.ErrorIs(t, err, outage)
	assert.ErrorContains(t, err, "read retained check coverage octo/repo#7")
	assert.Nil(t, response)
}

// A missing check whose name is already taken by an untrusted app's Check Run
// is reported distinctly, so the operator knows backfill alone leaves a
// conflicting check to resolve.
func TestScanWebhookMissingChecksSurfacesUntrustedConflicts(t *testing.T) {
	t.Parallel()

	client := fakeWebhookMissingCheckScanClient{
		prs: []ghclient.OpenPullRequest{
			{Number: 5, Title: "untrusted conflict", HeadSHA: "sha5", HeadRef: "feature-5"},
		},
		untrusted: map[string][]string{
			"sha5/SchemaBot (production)": {"some-other-app"},
		},
	}

	result, err := scanWebhookMissingChecks(t.Context(), client, nil, "octo/repo", productionCheckName, 0, time.Time{}, 0, discardLogger())

	require.NoError(t, err)
	require.Len(t, result.Missing, 1)
	assert.Equal(t, []string{"SchemaBot (production)"}, result.Missing[0].MissingNames)
	assert.Equal(t, []string{"SchemaBot (production)"}, result.Missing[0].UntrustedConflictNames)
}

// A stuck Check Run carries the stored rows behind it, so a fleet sweep can
// tell the two stuck shapes apart without opening the pull request: a row a
// plan or an apply will still resolve, and a row that needs a person. The
// entry is classified by its worst row, because an entry reported as
// self-converging when part of it is not reads as safe to leave alone.
func TestScanWebhookMissingChecksExplainsStuckRunsFromStoredState(t *testing.T) {
	t.Parallel()

	client := fakeWebhookMissingCheckScanClient{
		prs: []ghclient.OpenPullRequest{
			{Number: 11, Title: "wedged", HeadSHA: "sha11", HeadRef: "feature-11"},
		},
		runs: map[string]*ghclient.CheckRunResult{
			"sha11/SchemaBot (production)": {ID: 110, Name: "SchemaBot (production)", Status: "in_progress"},
		},
	}
	store := &inspectStorage{
		checks: &inspectCheckStore{checks: []*storage.Check{
			{Environment: "production", DatabaseName: "widgets", HeadSHA: "older", ApplyID: 5,
				Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionSuccess},
			{Environment: "production", DatabaseName: "gadgets", HeadSHA: "sha11", ApplyID: 6,
				Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionActionRequired,
				BlockingReason: checkstate.BlockRollbackCompleted},
		}},
		applies: &inspectApplyStore{applies: map[int64]*storage.Apply{
			5: {ApplyIdentifier: "apply-5", State: "completed"},
			6: {ApplyIdentifier: "apply-6", State: "rolled_back"},
		}},
	}

	result, err := scanWebhookMissingChecks(t.Context(), client, store, "octo/repo", productionCheckName, 0, time.Time{}, 0, discardLogger())

	require.NoError(t, err)
	require.Len(t, result.Stuck, 1)
	stuck := result.Stuck[0]
	assert.Equal(t, apitypes.WaitingOnOperator, stuck.Checks[0].WaitingOn, "one row needing a person classifies the whole entry")
	require.Len(t, stuck.Checks[0].StoredRows, 2)
	assert.Equal(t, "gadgets", stuck.Checks[0].StoredRows[0].Database, "rows are ordered by environment then database")
	assert.Equal(t, checkstate.ReasonReconciliationOwed, stuck.Checks[0].StoredRows[0].Reason)
	assert.Equal(t, "apply-6", stuck.Checks[0].StoredRows[0].ApplyIdentifier)
	assert.Equal(t, "widgets", stuck.Checks[0].StoredRows[1].Database)
	assert.Equal(t, checkstate.ReasonAwaitingReplanAfterApply, stuck.Checks[0].StoredRows[1].Reason)
}

// Every stored row resolving on its own is the shape an operator can skip,
// and the sweep says so rather than leaving the entry unclassified.
func TestScanWebhookMissingChecksMarksSelfConvergingStuckRuns(t *testing.T) {
	t.Parallel()

	client := fakeWebhookMissingCheckScanClient{
		prs: []ghclient.OpenPullRequest{
			{Number: 12, Title: "waiting", HeadSHA: "sha12", HeadRef: "feature-12"},
		},
		runs: map[string]*ghclient.CheckRunResult{
			"sha12/SchemaBot (production)": {ID: 120, Name: "SchemaBot (production)", Status: "in_progress"},
		},
	}
	store := &inspectStorage{
		checks: &inspectCheckStore{checks: []*storage.Check{
			{Environment: "production", DatabaseName: "widgets", HeadSHA: "sha12", ApplyID: 7,
				Status: checkstate.StatusInProgress},
		}},
		applies: &inspectApplyStore{applies: map[int64]*storage.Apply{
			7: {ApplyIdentifier: "apply-7", State: "running"},
		}},
	}

	result, err := scanWebhookMissingChecks(t.Context(), client, store, "octo/repo", productionCheckName, 0, time.Time{}, 0, discardLogger())

	require.NoError(t, err)
	require.Len(t, result.Stuck, 1)
	assert.Equal(t, apitypes.WaitingOnSchemaBot, result.Stuck[0].Checks[0].WaitingOn)
	require.Len(t, result.Stuck[0].Checks[0].StoredRows, 1)
	assert.Equal(t, checkstate.ReasonApplyRunning, result.Stuck[0].Checks[0].StoredRows[0].Reason)
}

// A pull request carries one Check Run per environment and each gates merge
// on its own, so each stuck run is explained by the rows for the environment
// it gates. Attributing another environment's rows to it would name a cause
// that has nothing to do with why it is sitting, and would send an operator
// after the wrong deployment.
func TestScanWebhookMissingChecksScopesStoredRowsToEachRunsEnvironment(t *testing.T) {
	t.Parallel()

	client := fakeWebhookMissingCheckScanClient{
		prs: []ghclient.OpenPullRequest{
			{Number: 14, Title: "two gates", HeadSHA: "sha14", HeadRef: "feature-14"},
		},
		runs: map[string]*ghclient.CheckRunResult{
			"sha14/SchemaBot (staging)":    {ID: 140, Name: "SchemaBot (staging)", Status: "in_progress"},
			"sha14/SchemaBot (production)": {ID: 141, Name: "SchemaBot (production)", Status: "in_progress"},
		},
	}
	store := &inspectStorage{
		checks: &inspectCheckStore{checks: []*storage.Check{
			// Staging is waiting out an apply SchemaBot finishes on its own.
			{Environment: "staging", DatabaseName: "widgets", HeadSHA: "sha14", ApplyID: 8,
				Status: checkstate.StatusInProgress},
			// Production owes a person a reconciliation.
			{Environment: "production", DatabaseName: "widgets", HeadSHA: "sha14", ApplyID: 9,
				Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionActionRequired,
				BlockingReason: checkstate.BlockRollbackCompleted},
		}},
		applies: &inspectApplyStore{applies: map[int64]*storage.Apply{
			8: {ApplyIdentifier: "apply-8", State: "running"},
			9: {ApplyIdentifier: "apply-9", State: "rolled_back"},
		}},
	}
	names := []webhookExpectedCheckName{
		{Name: "SchemaBot (staging)", Environment: "staging"},
		{Name: "SchemaBot (production)", Environment: "production"},
	}

	result, err := scanWebhookMissingChecks(t.Context(), client, store, "octo/repo", names, 0, time.Time{}, 0, discardLogger())

	require.NoError(t, err)
	require.Len(t, result.Stuck, 1)
	require.Len(t, result.Stuck[0].Checks, 2)

	staging := result.Stuck[0].Checks[0]
	require.Equal(t, "SchemaBot (staging)", staging.Name)
	require.Len(t, staging.StoredRows, 1, "the production row belongs to the other run")
	assert.Equal(t, "staging", staging.StoredRows[0].Environment)
	assert.Equal(t, checkstate.ReasonApplyRunning, staging.StoredRows[0].Reason)
	assert.Equal(t, apitypes.WaitingOnSchemaBot, staging.WaitingOn,
		"the production reconciliation must not make the staging gate look operator-owned")

	production := result.Stuck[0].Checks[1]
	require.Equal(t, "SchemaBot (production)", production.Name)
	require.Len(t, production.StoredRows, 1)
	assert.Equal(t, "production", production.StoredRows[0].Environment)
	assert.Equal(t, checkstate.ReasonReconciliationOwed, production.StoredRows[0].Reason)
	assert.Equal(t, apitypes.WaitingOnOperator, production.WaitingOn)

	// The rows are one pull request's, so they are read once and scoped per
	// run. Reading them per run instead would return the same annotations at
	// twice the cost, on a sweep that already pages through every open PR in
	// the fleet.
	assert.Equal(t, 1, store.checks.reads,
		"one read serves every annotated run on a pull request")
}

func TestScanWebhookMissingChecksIncludesPrerequisiteEvidence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		checks     []*storage.Check
		wantReason string
		wantWait   string
	}{
		{
			name: "missing predecessor",
			checks: []*storage.Check{{Environment: "production", DatabaseType: "mysql", DatabaseName: "orders", HeadSHA: "sha14",
				Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionSuccess}},
			wantReason: checkstate.ReasonAwaitingPlan, wantWait: apitypes.WaitingOnSchemaBot,
		},
		{
			name: "retained predecessor without later result",
			checks: []*storage.Check{{Environment: "sandbox", DatabaseType: "mysql", DatabaseName: "orders", HeadSHA: "sha14",
				Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionActionRequired}},
			wantReason: checkstate.ReasonBlocked, wantWait: apitypes.WaitingOnOperator,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := &ServerConfig{
				AllowedEnvironments: []string{"sandbox", "production"}, EnvironmentOrder: []string{"sandbox", "production"},
				Databases: map[string]DatabaseConfig{"orders": {Type: "mysql", AllowedRepos: []string{"octo/repo"},
					Environments: map[string]EnvironmentConfig{"sandbox": {}, "production": {}}}},
			}
			client := fakeWebhookMissingCheckScanClient{
				prs: []ghclient.OpenPullRequest{{Number: 14, HeadSHA: "sha14"}},
				runs: map[string]*ghclient.CheckRunResult{
					"sha14/SchemaBot (production)": {ID: 141, Name: "SchemaBot (production)", Status: "in_progress"},
				},
			}
			store := &inspectStorage{checks: &inspectCheckStore{checks: tc.checks}, applies: &inspectApplyStore{}}
			result, err := scanWebhookMissingChecks(t.Context(), client, store, "octo/repo", productionCheckName, 0, time.Time{}, 0, discardLogger(), checkScanScope{config: cfg, environment: "production"})
			require.NoError(t, err)
			require.Len(t, result.Stuck, 1)
			require.Len(t, result.Stuck[0].Checks, 1)
			check := result.Stuck[0].Checks[0]
			assert.Equal(t, tc.wantWait, check.WaitingOn)
			require.Len(t, check.StoredRows, 2)
			assert.Equal(t, "production", check.StoredRows[0].Environment)
			assert.Equal(t, "sandbox", check.StoredRows[1].Environment)
			assert.Equal(t, "orders", check.StoredRows[1].Database)
			assert.Equal(t, tc.wantReason, check.StoredRows[1].Reason)
			assert.True(t, check.StoredRows[1].Blocking)
		})
	}
}

// A deployment publishing a single unscoped check gates every environment on
// one Check Run, so that run is explained by every stored row on the pull
// request. Narrowing it to one environment would blank the annotation for the
// deployments that have no per-environment names at all.
func TestScanWebhookMissingChecksAnnotatesAnUnscopedRunFromEveryRow(t *testing.T) {
	t.Parallel()

	client := fakeWebhookMissingCheckScanClient{
		prs: []ghclient.OpenPullRequest{
			{Number: 16, Title: "one gate", HeadSHA: "sha16", HeadRef: "feature-16"},
		},
		runs: map[string]*ghclient.CheckRunResult{
			"sha16/SchemaBot": {ID: 160, Name: "SchemaBot", Status: "in_progress"},
		},
	}
	store := &inspectStorage{
		checks: &inspectCheckStore{checks: []*storage.Check{
			{Environment: "staging", DatabaseName: "widgets", HeadSHA: "sha16", ApplyID: 8,
				Status: checkstate.StatusInProgress},
			{Environment: "production", DatabaseName: "widgets", HeadSHA: "sha16", ApplyID: 9,
				Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionActionRequired,
				BlockingReason: checkstate.BlockRollbackCompleted},
		}},
		applies: &inspectApplyStore{applies: map[int64]*storage.Apply{
			8: {ApplyIdentifier: "apply-8", State: "running"},
			9: {ApplyIdentifier: "apply-9", State: "rolled_back"},
		}},
	}
	names := []webhookExpectedCheckName{{Name: "SchemaBot"}}

	result, err := scanWebhookMissingChecks(t.Context(), client, store, "octo/repo", names, 0, time.Time{}, 0, discardLogger())

	require.NoError(t, err)
	require.Len(t, result.Stuck, 1)
	require.Len(t, result.Stuck[0].Checks, 1)
	stuck := result.Stuck[0].Checks[0]
	require.Len(t, stuck.StoredRows, 2, "one gate over every environment is explained by every row")
	assert.Equal(t, apitypes.WaitingOnOperator, stuck.WaitingOn,
		"a reconciliation anywhere behind the single gate is owed to a person")
}

// An operator-supplied check name reports on whatever environment they also
// named. Handing it every environment's rows would attribute one
// environment's reconciliation to a run gating another.
func TestWebhookExpectedCheckNamesScopesAnOverrideToTheRequestedEnvironment(t *testing.T) {
	t.Parallel()

	cfg := &ServerConfig{AllowedEnvironments: []string{"staging", "production"}}

	scoped := webhookExpectedCheckNames(cfg, "octo/repo", "staging", "SchemaBot (staging)")
	require.Len(t, scoped, 1)
	assert.Equal(t, "SchemaBot (staging)", scoped[0].Name)
	assert.Equal(t, "staging", scoped[0].Environment)

	unscoped := webhookExpectedCheckNames(cfg, "octo/repo", "", "Some Other Check")
	require.Len(t, unscoped, 1)
	assert.Empty(t, unscoped[0].Environment,
		"an override with no environment behind it covers whatever the operator meant")
}

// The stored-row annotation costs a read per pull request and one per apply
// behind it, and a caller that drops young runs never renders the result. A
// run below the caller's threshold is still reported, without paying for an
// explanation nobody sees.
func TestScanWebhookMissingChecksSkipsTheAnnotationForAYoungRun(t *testing.T) {
	t.Parallel()

	client := fakeWebhookMissingCheckScanClient{
		prs: []ghclient.OpenPullRequest{
			{Number: 17, Title: "just started", HeadSHA: "sha17", HeadRef: "feature-17"},
		},
		runs: map[string]*ghclient.CheckRunResult{
			"sha17/SchemaBot (production)": {ID: 170, Name: "SchemaBot (production)",
				Status: "in_progress", StartedAt: time.Now().UTC().Add(-time.Minute)},
		},
	}
	store := &inspectStorage{
		checks: &inspectCheckStore{checks: []*storage.Check{
			{Environment: "production", DatabaseName: "widgets", HeadSHA: "sha17", ApplyID: 9,
				Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionActionRequired,
				BlockingReason: checkstate.BlockRollbackCompleted},
		}},
		applies: &inspectApplyStore{applies: map[int64]*storage.Apply{
			9: {ApplyIdentifier: "apply-9", State: "rolled_back"},
		}},
	}
	names := []webhookExpectedCheckName{{Name: "SchemaBot (production)", Environment: "production"}}

	result, err := scanWebhookMissingChecks(t.Context(), client, store, "octo/repo", names, 0, time.Time{}, time.Hour, discardLogger())

	require.NoError(t, err)
	require.Len(t, result.Stuck, 1)
	require.Len(t, result.Stuck[0].Checks, 1)
	stuck := result.Stuck[0].Checks[0]
	assert.Equal(t, "SchemaBot (production)", stuck.Name, "the run is still reported")
	assert.Empty(t, stuck.StoredRows)
	assert.Empty(t, stuck.WaitingOn)
	assert.Zero(t, store.checks.reads,
		"a pull request whose every run is too young to render pays for no stored read")

	// A run whose age cannot be established is one the caller renders, so it
	// is annotated whatever the threshold says.
	client.runs["sha17/SchemaBot (production)"] = &ghclient.CheckRunResult{
		ID: 170, Name: "SchemaBot (production)", Status: "in_progress",
	}
	result, err = scanWebhookMissingChecks(t.Context(), client, store, "octo/repo", names, 0, time.Time{}, time.Hour, discardLogger())
	require.NoError(t, err)
	require.Len(t, result.Stuck, 1)
	require.Len(t, result.Stuck[0].Checks, 1)
	assert.Equal(t, apitypes.WaitingOnOperator, result.Stuck[0].Checks[0].WaitingOn)
}

// The caller ages a run from the start time it reads back, applying the same
// threshold, to decide what to render. The wire carries whole seconds, so a run
// judged here against the untruncated instant GitHub reported is measured from
// slightly later than the caller measures it from — and at the boundary that is
// a run this scan leaves unannotated and the caller renders anyway, with an
// empty waiting-on column that means no stored row was blocking rather than
// that none was read.
func TestCheckRunAgedForAnnotationJudgesTheStartTimeTheCallerReadsBack(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)
	// 59m59.6s old by GitHub's clock, a flat hour by the wire's.
	run := &ghclient.CheckRunResult{StartedAt: now.Add(-time.Hour).Add(400 * time.Millisecond)}

	startedAt, sittingLongEnough := checkRunAgedForAnnotation(run, time.Hour, now)

	require.Equal(t, "2026-07-12T11:00:00Z", startedAt)
	parsed, err := time.Parse(time.RFC3339, startedAt)
	require.NoError(t, err)
	require.GreaterOrEqual(t, now.Sub(parsed), time.Hour,
		"what the caller computes from the reported start time")
	assert.True(t, sittingLongEnough,
		"the caller will render this run, so the rows explaining it have to be read")
}

// The clock a page is judged against is reported at whole-second precision, so
// it is read at that precision too. Judged finer than it is reported, the
// caller's copy trails the instant the decision was made on, and a run inside
// that fraction of a threshold finer than a second is annotated by this scan
// and then dropped by the caller — losing the annotation on the only surface
// that shows it.
func TestScanObservedNowIsReadAtThePrecisionItIsReportedAt(t *testing.T) {
	t.Parallel()

	observed := scanObservedNow()

	assert.Zero(t, observed.Nanosecond(), "a finer clock than the wire carries cannot be handed back")
	require.Equal(t, observed, mustParseRFC3339(t, observed.Format(time.RFC3339)),
		"the reported clock round-trips to the one the runs were judged against")
}

func mustParseRFC3339(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, value)
	require.NoError(t, err)
	return parsed.UTC()
}

// The clock the runs were aged against is reported, so a caller applying the
// same threshold after the round trip reaches the same verdict instead of
// judging every run slightly older than this scan did.
func TestScanWebhookMissingChecksReportsTheClockItAgedTheRunsAgainst(t *testing.T) {
	t.Parallel()

	client := fakeWebhookMissingCheckScanClient{
		prs: []ghclient.OpenPullRequest{
			{Number: 23, Title: "one uncompleted run", HeadSHA: "sha23", HeadRef: "feature-23"},
		},
		runs: map[string]*ghclient.CheckRunResult{
			"sha23/SchemaBot": {ID: 230, Name: "SchemaBot", Status: "in_progress",
				StartedAt: time.Now().UTC().Add(-2 * time.Hour)},
		},
	}
	store := &inspectStorage{checks: &inspectCheckStore{}, applies: &inspectApplyStore{}}

	before := time.Now().UTC().Truncate(time.Second)
	result, err := scanWebhookMissingChecks(t.Context(), client, store, "octo/repo",
		[]webhookExpectedCheckName{{Name: "SchemaBot"}}, 0, time.Time{}, time.Hour, discardLogger())
	require.NoError(t, err)

	observed, parseErr := time.Parse(time.RFC3339, result.ObservedAt)
	require.NoError(t, parseErr, "the clock the runs were aged against has to be readable")
	assert.False(t, observed.Before(before), "the reported clock is the one this scan used")
}

// A pull request can carry an old uncompleted run beside a young one, and the
// old one is exactly what the caller renders. The age threshold decides each
// run on its own, so the young one never speaks for both: an operator looking
// at the old run gets the stored state explaining it.
func TestScanWebhookMissingChecksAnnotatesAnOldRunBesideAYoungOne(t *testing.T) {
	t.Parallel()

	client := fakeWebhookMissingCheckScanClient{
		prs: []ghclient.OpenPullRequest{
			{Number: 19, Title: "one old, one young", HeadSHA: "sha19", HeadRef: "feature-19"},
		},
		runs: map[string]*ghclient.CheckRunResult{
			"sha19/SchemaBot (production)": {ID: 190, Name: "SchemaBot (production)",
				Status: "in_progress", StartedAt: time.Now().UTC().Add(-24 * time.Hour)},
			"sha19/SchemaBot (staging)": {ID: 191, Name: "SchemaBot (staging)",
				Status: "in_progress", StartedAt: time.Now().UTC().Add(-time.Minute)},
		},
	}
	store := &inspectStorage{
		checks: &inspectCheckStore{checks: []*storage.Check{
			{Environment: "production", DatabaseName: "widgets", HeadSHA: "sha19", ApplyID: 9,
				Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionActionRequired,
				BlockingReason: checkstate.BlockRollbackCompleted},
			{Environment: "staging", DatabaseName: "widgets", HeadSHA: "sha19", ApplyID: 9,
				Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionActionRequired,
				BlockingReason: checkstate.BlockRollbackCompleted},
		}},
		applies: &inspectApplyStore{applies: map[int64]*storage.Apply{
			9: {ApplyIdentifier: "apply-9", State: "rolled_back"},
		}},
	}
	names := []webhookExpectedCheckName{
		{Name: "SchemaBot (production)", Environment: "production"},
		{Name: "SchemaBot (staging)", Environment: "staging"},
	}

	result, err := scanWebhookMissingChecks(t.Context(), client, store, "octo/repo", names, 0, time.Time{}, time.Hour, discardLogger())

	require.NoError(t, err)
	require.Len(t, result.Stuck, 1)
	require.Len(t, result.Stuck[0].Checks, 2)

	annotated := map[string]string{}
	for _, run := range result.Stuck[0].Checks {
		annotated[run.Name] = run.WaitingOn
	}
	assert.Equal(t, apitypes.WaitingOnOperator, annotated["SchemaBot (production)"],
		"the old run is the one the caller renders, so it carries its stored state")
	assert.Empty(t, annotated["SchemaBot (staging)"],
		"the young run is still reported, just unannotated")
}

// The rollup restates the rows beside it, so it is never what an operator
// acts on. A run whose only blocking row is the rollup has nothing recorded
// that explains it, and saying "schemabot" there would promise a convergence
// nothing is going to deliver.
func TestScanWebhookMissingChecksDoesNotClassifyAStuckRunFromTheRollupAlone(t *testing.T) {
	t.Parallel()

	client := fakeWebhookMissingCheckScanClient{
		prs: []ghclient.OpenPullRequest{
			{Number: 15, Title: "rollup only", HeadSHA: "sha15", HeadRef: "feature-15"},
		},
		runs: map[string]*ghclient.CheckRunResult{
			"sha15/SchemaBot (production)": {ID: 150, Name: "SchemaBot (production)", Status: "in_progress"},
		},
	}
	store := &inspectStorage{
		checks: &inspectCheckStore{checks: []*storage.Check{
			{Environment: "production", DatabaseType: checkstate.AggregateSentinel,
				DatabaseName: checkstate.AggregateSentinel, HeadSHA: "sha15",
				Status: checkstate.StatusInProgress},
		}},
		applies: &inspectApplyStore{applies: map[int64]*storage.Apply{}},
	}

	result, err := scanWebhookMissingChecks(t.Context(), client, store, "octo/repo", productionCheckName, 0, time.Time{}, 0, discardLogger())

	require.NoError(t, err)
	require.Len(t, result.Stuck, 1)
	require.Len(t, result.Stuck[0].Checks[0].StoredRows, 1, "the rollup is still reported, just never a cause")
	assert.True(t, result.Stuck[0].Checks[0].StoredRows[0].Aggregate)
	assert.Empty(t, result.Stuck[0].Checks[0].WaitingOn)
}

// Stored state that cannot be read costs the entry its explanation and
// nothing else. The Check Run findings are what the backfill acts on and
// they are already in hand, so the scan still reports the stuck run.
func TestScanWebhookMissingChecksReportsStuckRunsWhenStoredStateIsUnreadable(t *testing.T) {
	t.Parallel()

	client := fakeWebhookMissingCheckScanClient{
		prs: []ghclient.OpenPullRequest{
			{Number: 13, Title: "wedged", HeadSHA: "sha13", HeadRef: "feature-13"},
		},
		runs: map[string]*ghclient.CheckRunResult{
			"sha13/SchemaBot (production)": {ID: 130, Name: "SchemaBot (production)", Status: "in_progress"},
		},
	}
	store := &inspectStorage{checks: &inspectCheckStore{err: errors.New("storage unavailable")}}

	result, err := scanWebhookMissingChecks(t.Context(), client, store, "octo/repo", productionCheckName, 0, time.Time{}, 0, discardLogger())

	require.NoError(t, err)
	require.Len(t, result.Stuck, 1)
	assert.Empty(t, result.Stuck[0].Checks[0].StoredRows)
	assert.Empty(t, result.Stuck[0].Checks[0].WaitingOn)
	require.Len(t, result.Stuck[0].Checks, 1, "the finding the backfill acts on survives the failed read")
}

// Inspection and backfill share the registered environment candidate set.
func TestWebhookExpectedCheckNamesUsesRegisteredEnvironments(t *testing.T) {
	cfg := &ServerConfig{AllowedEnvironments: []string{"staging", "sandbox", "production"}, Databases: map[string]DatabaseConfig{"orders": {Environments: map[string]EnvironmentConfig{"staging": {}}}}}
	assert.Equal(t, []webhookExpectedCheckName{
		{Name: "SchemaBot (staging)", Environment: "staging"},
	}, webhookExpectedCheckNames(cfg, "octocat/repo", "", ""))
}

// A scan of named PRs reads only those PRs instead of paging the repository,
// leaves out a named PR that is no longer open, and reports each one's
// missing checks exactly as a paged scan would.
func TestExecuteChecksScanOfNamedPRsReadsOnlyThosePRs(t *testing.T) {
	t.Parallel()

	cfg := &ServerConfig{
		Apps:  map[string]GitHubAppConfig{"main": {AppID: "1", PrivateKey: "key"}},
		Repos: map[string]RepoConfig{"octo/repo": {GitHubApp: "main"}},
	}
	client := fakeChecksScanClient{fakeWebhookMissingCheckScanClient{
		prs: []ghclient.OpenPullRequest{
			{Number: 1, HeadSHA: "sha1", Title: "one"},
			{Number: 2, HeadSHA: "sha2", Title: "two"},
			{Number: 3, HeadSHA: "sha3", Title: "three"},
		},
		runs: map[string]*ghclient.CheckRunResult{
			"sha3/SchemaBot": {ID: 30, Name: "SchemaBot", Status: "completed", Conclusion: "success"},
		},
	}}
	resolve := checksScanResolverByRepo(map[string]checksScanClient{"octo/repo": client}, nil)

	response, err := executeChecksScanWith(t.Context(), cfg, nil, ChecksScanRequest{Repo: "octo/repo", PRs: []int{2, 3, 99}}, discardLogger(), resolve)
	require.NoError(t, err)

	assert.Equal(t, 2, response.Scanned, "PR 99 is not open, so it is left out")
	assert.Zero(t, response.NextPage)
	require.Len(t, response.Missing, 1)
	assert.Equal(t, 2, response.Missing[0].Number)
	assert.Equal(t, []string{"SchemaBot"}, response.Missing[0].MissingNames)
}

// A scan of named PRs refuses the paging and time-window fields, which only
// apply to a scan of the repository's open PRs, and bounds how many PRs one
// request reads.
func TestExecuteChecksScanOfNamedPRsValidation(t *testing.T) {
	t.Parallel()

	cfg := &ServerConfig{}
	for name, tc := range map[string]struct {
		req  ChecksScanRequest
		want string
	}{
		"page":          {ChecksScanRequest{Repo: "octo/repo", PRs: []int{1}, Page: 2}, "page does not apply to a scan of named prs"},
		"updated since": {ChecksScanRequest{Repo: "octo/repo", PRs: []int{1}, UpdatedSince: "2026-11-19T00:00:00Z"}, "updated_since does not apply to a scan of named prs"},
		"non-positive":  {ChecksScanRequest{Repo: "octo/repo", PRs: []int{1, 0}}, "pr numbers must be positive; got 0"},
		"too many":      {ChecksScanRequest{Repo: "octo/repo", PRs: make([]int, checksScanMaxNamedPRs+1)}, "at most 30 prs per scan"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := executeChecksScan(t.Context(), cfg, nil, tc.req, discardLogger())
			require.ErrorContains(t, err, tc.want)
		})
	}
}

// testMergeQueue is a merge queue testing two merge groups on main.
var testMergeQueue = []ghclient.MergeQueueHead{
	{Ref: "gh-readonly-queue/main/pr-412-0a1b2c3d", BaseRef: "main", PR: 412, HeadSHA: "group-412"},
	{Ref: "gh-readonly-queue/main/pr-418-4e5f6a7b", BaseRef: "main", PR: 418, HeadSHA: "group-418"},
}

func checksScanTestConfig(repo RepoConfig) *ServerConfig {
	return &ServerConfig{
		Apps:  map[string]GitHubAppConfig{"main": {AppID: "1", PrivateKey: "key"}},
		Repos: map[string]RepoConfig{"octo/repo": repo},
	}
}

// A first-page scan that asks for the merge queue reports each merge group
// missing a SchemaBot check, with the PR that created it, alongside the open
// PRs. A group that already has the check is not reported.
func TestExecuteChecksScanReportsMergeGroupsMissingChecks(t *testing.T) {
	t.Parallel()

	client := fakeChecksScanClient{fakeWebhookMissingCheckScanClient{
		queue: testMergeQueue,
		runs: map[string]*ghclient.CheckRunResult{
			"group-418/SchemaBot": {ID: 40, Name: "SchemaBot", Status: "completed", Conclusion: "success"},
		},
	}}
	resolve := checksScanResolverByRepo(map[string]checksScanClient{"octo/repo": client}, nil)

	response, err := executeChecksScanWith(t.Context(), checksScanTestConfig(RepoConfig{GitHubApp: "main"}), nil,
		ChecksScanRequest{Repo: "octo/repo", MergeQueue: true}, discardLogger(), resolve)
	require.NoError(t, err)

	assert.Equal(t, 2, response.MergeQueueScanned)
	require.Len(t, response.MergeQueue, 1)
	assert.Equal(t, MissingMergeQueueCheck{
		Ref:          "gh-readonly-queue/main/pr-412-0a1b2c3d",
		BaseRef:      "main",
		PR:           412,
		URL:          "https://github.com/octo/repo/pull/412",
		HeadSHA:      "group-412",
		MissingNames: []string{"SchemaBot"},
	}, response.MergeQueue[0])
}

// A paged sweep reads the merge queue once, on its first page; later pages
// and scans that did not ask for the queue never list it.
func TestExecuteChecksScanReadsTheMergeQueueOnlyOnTheFirstPage(t *testing.T) {
	t.Parallel()

	client := fakeChecksScanClient{fakeWebhookMissingCheckScanClient{queueErr: errors.New("merge queue must not be listed")}}
	resolve := checksScanResolverByRepo(map[string]checksScanClient{"octo/repo": client}, nil)
	cfg := checksScanTestConfig(RepoConfig{GitHubApp: "main"})

	for name, req := range map[string]ChecksScanRequest{
		"second page":         {Repo: "octo/repo", MergeQueue: true, Page: 2},
		"queue not requested": {Repo: "octo/repo"},
	} {
		t.Run(name, func(t *testing.T) {
			response, err := executeChecksScanWith(t.Context(), cfg, nil, req, discardLogger(), resolve)
			require.NoError(t, err)
			assert.Empty(t, response.MergeQueueError, "the queue was listed")
			assert.Zero(t, response.MergeQueueScanned)
			assert.Empty(t, response.MergeQueue)
		})
	}
}

// A merge queue that cannot be listed is reported as unread rather than as an
// empty queue, so a sweep never reports a blocked queue as clear. The PRs on
// the page are still scanned, so one repository's queue cannot stop the PRs'
// recovery in it or in the repositories after it.
func TestExecuteChecksScanReportsAMergeQueueItCannotList(t *testing.T) {
	t.Parallel()

	client := fakeChecksScanClient{fakeWebhookMissingCheckScanClient{queueErr: errors.New("502 Bad Gateway")}}
	resolve := checksScanResolverByRepo(map[string]checksScanClient{"octo/repo": client}, nil)

	response, err := executeChecksScanWith(t.Context(), checksScanTestConfig(RepoConfig{GitHubApp: "main"}), nil,
		ChecksScanRequest{Repo: "octo/repo", MergeQueue: true}, discardLogger(), resolve)
	require.NoError(t, err)
	assert.Equal(t, mergeQueueUnreadText, response.MergeQueueError)
	assert.Zero(t, response.MergeQueueScanned)
	assert.Empty(t, response.MergeQueue)
}

// An aggregate participant posts no checks on merge groups (its leader posts
// the required one), so its queue is not scanned: every group would read as
// missing a check that is not supposed to exist.
func TestExecuteChecksScanLeavesOutAParticipantsMergeQueue(t *testing.T) {
	t.Parallel()

	client := fakeChecksScanClient{fakeWebhookMissingCheckScanClient{queue: testMergeQueue}}
	resolve := checksScanResolverByRepo(map[string]checksScanClient{"octo/repo": client}, nil)
	cfg := checksScanTestConfig(RepoConfig{GitHubApp: "main", Aggregate: &AggregateConfig{Role: AggregateRoleParticipant}})

	response, err := executeChecksScanWith(t.Context(), cfg, nil, ChecksScanRequest{Repo: "octo/repo", MergeQueue: true}, discardLogger(), resolve)
	require.NoError(t, err)
	assert.Zero(t, response.MergeQueueScanned)
	assert.Empty(t, response.MergeQueue)
}

// A scan of named PRs refuses the merge queue: the queue belongs to the
// repository, not to the PRs the operator named.
func TestExecuteChecksScanOfNamedPRsRefusesTheMergeQueue(t *testing.T) {
	t.Parallel()

	_, err := executeChecksScan(t.Context(), &ServerConfig{}, nil,
		ChecksScanRequest{Repo: "octo/repo", PRs: []int{1}, MergeQueue: true}, discardLogger())
	require.ErrorContains(t, err, "merge_queue does not apply to a scan of named prs")
}

// The passing merge queue check goes only on a head the queue is still
// testing when the request arrives. A head the caller sends that the queue no
// longer holds gets nothing, and one head whose post fails does not stop the
// rest.
func TestBackfillMergeQueuePostsOnlyOnLiveHeads(t *testing.T) {
	t.Parallel()

	client := &fakeMergeQueueSynthesizeClient{fakeCoreRateLimit: fakeCoreRateLimit{remaining: []int{5000}, limit: 5000}, queue: testMergeQueue}
	backfiller := &fakeCheckRunBackfiller{failHead: "group-418"}

	results := backfillMergeQueueWithinBudget(t.Context(), client, backfiller,
		ChecksSynthesizeRequest{Repo: "octo/repo", MergeQueueHeads: []string{"group-412", "merged-group", "group-418"}, Environment: "production", CheckName: "SchemaBot (production)"}, 7, discardLogger())

	assert.Equal(t, []string{"group-412", "group-418"}, backfiller.mergeQueueCalls)
	scope := MergeQueueCheckScope{Environment: "production", CheckName: "SchemaBot (production)"}
	assert.Equal(t, []MergeQueueCheckScope{scope, scope}, backfiller.mergeQueueScopes, "each post carries the run's scope, so it posts only the checks the scan looked for")
	assert.Equal(t, []ChecksMergeQueueResult{
		{HeadSHA: "group-412", Outcome: "posted passing merge queue check"},
		{HeadSHA: "merged-group", Outcome: mergeQueueHeadGoneOutcome},
		{HeadSHA: "group-418", Error: mergeQueueBackfillFailedText},
	}, results)
}

// If the queue cannot be listed, no head can be verified, so nothing is
// posted and every head reports why (MG-1).
func TestBackfillMergeQueuePostsNothingWhenTheQueueCannotBeListed(t *testing.T) {
	t.Parallel()

	client := &fakeMergeQueueSynthesizeClient{queueErr: errors.New("502 Bad Gateway")}
	backfiller := &fakeCheckRunBackfiller{}

	results := backfillMergeQueueWithinBudget(t.Context(), client, backfiller,
		ChecksSynthesizeRequest{Repo: "octo/repo", MergeQueueHeads: []string{"group-412", "group-418"}}, 7, discardLogger())

	assert.Empty(t, backfiller.mergeQueueCalls)
	assert.Equal(t, []ChecksMergeQueueResult{
		{HeadSHA: "group-412", Error: mergeQueueUnlistedText},
		{HeadSHA: "group-418", Error: mergeQueueUnlistedText},
	}, results)
}

// A gh-readonly-queue/ branch proves only that a branch points at the commit,
// not that the merge queue made it. A head that is also an open PR's head is
// refused, since the passing check would pass that PR's own required check,
// and a head that cannot be checked gets nothing either (MG-1).
func TestBackfillMergeQueueRefusesAnOpenPRsHead(t *testing.T) {
	t.Parallel()

	client := &fakeMergeQueueSynthesizeClient{
		fakeCoreRateLimit: fakeCoreRateLimit{remaining: []int{5000}, limit: 5000},
		queue:             testMergeQueue,
		prHeads:           map[string]int{"group-418": 418},
	}
	backfiller := &fakeCheckRunBackfiller{}

	results := backfillMergeQueueWithinBudget(t.Context(), client, backfiller,
		ChecksSynthesizeRequest{Repo: "octo/repo", MergeQueueHeads: []string{"group-412", "group-418"}}, 7, discardLogger())

	assert.Equal(t, []string{"group-412"}, backfiller.mergeQueueCalls)
	assert.Equal(t, []ChecksMergeQueueResult{
		{HeadSHA: "group-412", Outcome: "posted passing merge queue check"},
		{HeadSHA: "group-418", Error: "refused: this commit is also the head of open PR #418, and a passing merge queue check would pass that PR's required check"},
	}, results)

	failing := &fakeMergeQueueSynthesizeClient{
		fakeCoreRateLimit: fakeCoreRateLimit{remaining: []int{5000}, limit: 5000},
		queue:             testMergeQueue,
		prHeadErr:         errors.New("502 Bad Gateway"),
	}
	backfiller = &fakeCheckRunBackfiller{}
	results = backfillMergeQueueWithinBudget(t.Context(), failing, backfiller,
		ChecksSynthesizeRequest{Repo: "octo/repo", MergeQueueHeads: []string{"group-412"}}, 7, discardLogger())

	assert.Empty(t, backfiller.mergeQueueCalls)
	assert.Equal(t, []ChecksMergeQueueResult{
		{HeadSHA: "group-412", Error: openPRHeadUncheckedText},
	}, results)
}

// Merge queue heads stop at the budget floor like PRs do, and the ones not
// started come back marked RateLimited for the caller to resend.
func TestBackfillMergeQueueStopsAtTheFloor(t *testing.T) {
	t.Parallel()

	client := &fakeMergeQueueSynthesizeClient{fakeCoreRateLimit: fakeCoreRateLimit{remaining: []int{3000, 900}, limit: 5000}, queue: testMergeQueue}
	backfiller := &fakeCheckRunBackfiller{}

	results := backfillMergeQueueWithinBudget(t.Context(), client, backfiller,
		ChecksSynthesizeRequest{Repo: "octo/repo", MergeQueueHeads: []string{"group-412", "group-418"}, RateLimitFloorPercent: 20}, 7, discardLogger())

	assert.Equal(t, []string{"group-412"}, backfiller.mergeQueueCalls)
	assert.Equal(t, []ChecksMergeQueueResult{
		{HeadSHA: "group-412", Outcome: "posted passing merge queue check"},
		{HeadSHA: "group-418", Outcome: "not started: GitHub budget is below 20% (900 of 5000 requests left)", RateLimited: true},
	}, results)
}

// A repository with Check Run publishing turned off skips its merge queue
// heads the same way it skips its PRs, without touching GitHub.
func TestExecuteChecksSynthesizeSkipsMergeQueueWithChecksDisabled(t *testing.T) {
	t.Parallel()

	checksOff := false
	backfiller := &fakeCheckRunBackfiller{}

	response, err := executeChecksSynthesize(t.Context(), checksScanTestConfig(RepoConfig{GitHubApp: "main", EnableChecks: &checksOff}), backfiller,
		ChecksSynthesizeRequest{Repo: "octo/repo", MergeQueueHeads: []string{"group-412"}}, discardLogger())
	require.NoError(t, err)
	assert.Empty(t, backfiller.mergeQueueCalls)
	assert.Equal(t, []ChecksMergeQueueResult{{HeadSHA: "group-412", Outcome: checksDisabledSkipOutcome}}, response.MergeQueue)
	assert.Empty(t, response.Results)
}
