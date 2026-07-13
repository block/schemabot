package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ghclient "github.com/block/schemabot/pkg/github"
)

type fakeWebhookRedriveDeliveryClient struct {
	pages              [][]*gh.HookDelivery
	details            map[int64]*gh.HookDelivery
	redelivered        []int64
	afterRedeliverHook func(deliveryID int64)
}

func (f *fakeWebhookRedriveDeliveryClient) ListHookDeliveries(_ context.Context, opts *gh.ListCursorOptions) ([]*gh.HookDelivery, *gh.Response, error) {
	page := 0
	if opts != nil && opts.Cursor == "page-2" {
		page = 1
	}
	if page >= len(f.pages) {
		return nil, &gh.Response{}, nil
	}
	resp := &gh.Response{}
	if page+1 < len(f.pages) {
		resp.Cursor = "page-2"
	}
	return f.pages[page], resp, nil
}

func (f *fakeWebhookRedriveDeliveryClient) GetHookDelivery(_ context.Context, deliveryID int64) (*gh.HookDelivery, *gh.Response, error) {
	detail := f.details[deliveryID]
	if detail == nil {
		return nil, nil, errors.New("not found")
	}
	return detail, &gh.Response{}, nil
}

func (f *fakeWebhookRedriveDeliveryClient) RedeliverHookDelivery(_ context.Context, deliveryID int64) (*gh.HookDelivery, *gh.Response, error) {
	if deliveryID == 999 {
		return nil, nil, errors.New("boom")
	}
	f.redelivered = append(f.redelivered, deliveryID)
	if f.afterRedeliverHook != nil {
		f.afterRedeliverHook(deliveryID)
	}
	return &gh.HookDelivery{ID: new(deliveryID)}, &gh.Response{}, nil
}

func TestWebhookRedriveEventEligibleMatchesCheckCreatingEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		event  string
		action string
		want   bool
	}{
		{name: "pull request opened", event: "pull_request", action: "opened", want: true},
		{name: "pull request synchronize", event: "pull_request", action: "synchronize", want: true},
		{name: "check suite requested", event: "check_suite", action: "requested", want: true},
		{name: "merge group checks requested", event: "merge_group", action: "checks_requested", want: true},
		{name: "issue comment excluded", event: "issue_comment", action: "created", want: false},
		{name: "closed pull request excluded", event: "pull_request", action: "closed", want: false},
		{name: "destroyed merge group excluded", event: "merge_group", action: "destroyed", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			delivery := &gh.HookDelivery{ID: new(int64(123)), Event: new(tt.event), Action: new(tt.action)}
			assert.Equal(t, tt.want, webhookRedriveEventEligible(delivery))
		})
	}
}

// Delivery success is judged by the 2xx status code, not the literal status
// string: a 202 ("Accepted") is a success, not a failure to redrive.
func TestWebhookDeliverySucceededUsesStatusCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		code int
		want bool
	}{
		{name: "200 OK", code: 200, want: true},
		{name: "202 Accepted", code: 202, want: true},
		{name: "500 error", code: 500, want: false},
		{name: "408 timeout", code: 408, want: false},
		{name: "0 never delivered", code: 0, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, webhookDeliverySucceeded(&gh.HookDelivery{StatusCode: new(tt.code)}))
		})
	}
}

func TestRedriveWebhookAppDeliveriesDryRunSelectsFailedDeliveriesInWindow(t *testing.T) {
	t.Parallel()

	windowStart := time.Date(2026, 7, 7, 19, 40, 0, 0, time.UTC)
	windowEnd := time.Date(2026, 7, 7, 19, 50, 0, 0, time.UTC)
	client := &fakeWebhookRedriveDeliveryClient{pages: [][]*gh.HookDelivery{
		{
			webhookRedriveTestDelivery(101, windowEnd.Add(-time.Minute), "pull_request", "opened", "timed out", 504),
			webhookRedriveTestDelivery(102, windowEnd.Add(-2*time.Minute), "pull_request", "opened", "OK", 200),
		},
		{
			webhookRedriveTestDelivery(103, windowStart.Add(time.Minute), "issue_comment", "created", "ERROR", 500),
			webhookRedriveTestDelivery(104, windowStart.Add(-time.Minute), "pull_request", "synchronize", "ERROR", 500),
		},
	}}

	result, err := redriveWebhookAppDeliveries(t.Context(), client, "default", "", windowStart, windowEnd, 10, true, "", 0, discardLogger())

	require.NoError(t, err)
	require.Len(t, result.Selected, 1)
	assert.True(t, result.DryRun)
	assert.Equal(t, int64(101), result.Selected[0].ID)
	assert.Equal(t, 4, result.Fetched)
	assert.Equal(t, 2, result.Pages)
	assert.True(t, result.ReachedWindowStart)
	assert.Empty(t, client.redelivered)
}

func TestRedriveWebhookAppDeliveriesReturnsCursorWhenWindowStartNotReached(t *testing.T) {
	t.Parallel()

	windowStart := time.Date(2026, 7, 7, 19, 40, 0, 0, time.UTC)
	windowEnd := time.Date(2026, 7, 7, 19, 50, 0, 0, time.UTC)
	// Two pages exist but only one is allowed: the cursor is still available
	// when the page budget runs out, so the caller can continue the listing
	// with a follow-up request instead of restarting from the newest page.
	client := &fakeWebhookRedriveDeliveryClient{pages: [][]*gh.HookDelivery{
		{webhookRedriveTestDelivery(101, windowEnd.Add(-time.Minute), "pull_request", "opened", "ERROR", 500)},
		{webhookRedriveTestDelivery(102, windowEnd.Add(-2*time.Minute), "pull_request", "opened", "ERROR", 500)},
	}}

	result, err := redriveWebhookAppDeliveries(t.Context(), client, "default", "", windowStart, windowEnd, 1, true, "", 0, discardLogger())

	require.NoError(t, err)
	assert.False(t, result.ReachedWindowStart)
	assert.False(t, result.HistoryExhausted)
	assert.Equal(t, "page-2", result.NextCursor)
}

func TestRedriveWebhookAppDeliveriesRedeliversSelectedDeliveries(t *testing.T) {
	t.Parallel()

	windowStart := time.Date(2026, 7, 7, 19, 40, 0, 0, time.UTC)
	windowEnd := time.Date(2026, 7, 7, 19, 50, 0, 0, time.UTC)
	client := &fakeWebhookRedriveDeliveryClient{pages: [][]*gh.HookDelivery{
		{
			webhookRedriveTestDelivery(101, windowEnd.Add(-time.Minute), "pull_request", "opened", "ERROR", 500),
			webhookRedriveTestDelivery(102, windowStart.Add(-time.Minute), "pull_request", "opened", "ERROR", 500),
		},
	}}

	result, err := redriveWebhookAppDeliveries(t.Context(), client, "default", "", windowStart, windowEnd, 10, false, "", 0, discardLogger())

	require.NoError(t, err)
	assert.False(t, result.DryRun)
	assert.Equal(t, []int64{101}, client.redelivered)
	assert.Equal(t, 1, result.Redelivered)
	assert.Equal(t, 0, result.Failed)
}

func TestRedriveWebhookAppDeliveriesStopsPromptlyWhenDelayContextIsCanceled(t *testing.T) {
	t.Parallel()

	windowStart := time.Date(2026, 7, 7, 19, 40, 0, 0, time.UTC)
	windowEnd := time.Date(2026, 7, 7, 19, 50, 0, 0, time.UTC)
	ctx, cancel := context.WithCancel(t.Context())
	client := &fakeWebhookRedriveDeliveryClient{
		pages: [][]*gh.HookDelivery{{
			webhookRedriveTestDelivery(101, windowEnd.Add(-time.Minute), "pull_request", "opened", "ERROR", 500),
			webhookRedriveTestDelivery(102, windowEnd.Add(-2*time.Minute), "pull_request", "opened", "ERROR", 500),
			webhookRedriveTestDelivery(103, windowStart.Add(-time.Minute), "pull_request", "opened", "ERROR", 500),
		}},
		afterRedeliverHook: func(deliveryID int64) {
			if deliveryID == 101 {
				cancel()
			}
		},
	}

	result, err := redriveWebhookAppDeliveries(ctx, client, "default", "", windowStart, windowEnd, 10, false, "", 0, discardLogger())

	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, []int64{101}, client.redelivered)
	assert.Equal(t, 1, result.Redelivered)
}

func TestRedriveWebhookAppDeliveriesFiltersByRepoAndPullRequest(t *testing.T) {
	t.Parallel()

	windowStart := time.Date(2026, 7, 7, 19, 40, 0, 0, time.UTC)
	windowEnd := time.Date(2026, 7, 7, 19, 50, 0, 0, time.UTC)
	client := &fakeWebhookRedriveDeliveryClient{
		pages: [][]*gh.HookDelivery{{
			webhookRedriveTestDelivery(101, windowEnd.Add(-time.Minute), "pull_request", "opened", "ERROR", 500),
			webhookRedriveTestDelivery(102, windowEnd.Add(-2*time.Minute), "pull_request", "opened", "ERROR", 500),
			webhookRedriveTestDelivery(103, windowStart.Add(-time.Minute), "pull_request", "opened", "ERROR", 500),
		}},
		details: map[int64]*gh.HookDelivery{
			101: webhookRedriveTestDeliveryDetail(101, "octo/repo", 12),
			102: webhookRedriveTestDeliveryDetail(102, "octo/other", 12),
		},
	}

	result, err := redriveWebhookAppDeliveries(t.Context(), client, "default", "", windowStart, windowEnd, 10, true, "octo/repo", 12, discardLogger())

	require.NoError(t, err)
	require.Len(t, result.Selected, 1)
	assert.Equal(t, int64(101), result.Selected[0].ID)
	assert.Equal(t, "octo/repo", result.Selected[0].Repo)
	assert.Equal(t, 12, result.Selected[0].PR)
}

// A check_suite payload can carry several pull requests; a delivery is
// selected when any of them matches the PR filter, so a busy-repo check_suite
// touching the requested PR is not silently skipped.
func TestRedriveWebhookAppDeliveriesMatchesPRAmongMultiplePayloadPRs(t *testing.T) {
	t.Parallel()

	windowStart := time.Date(2026, 7, 7, 19, 40, 0, 0, time.UTC)
	windowEnd := time.Date(2026, 7, 7, 19, 50, 0, 0, time.UTC)
	multiPRPayload, err := json.Marshal(map[string]any{
		"repository": map[string]any{"full_name": "octo/repo"},
		"check_suite": map[string]any{
			"pull_requests": []map[string]any{{"number": 7}, {"number": 12}},
		},
	})
	require.NoError(t, err)
	raw := json.RawMessage(multiPRPayload)
	client := &fakeWebhookRedriveDeliveryClient{
		pages: [][]*gh.HookDelivery{{
			webhookRedriveTestDelivery(201, windowEnd.Add(-time.Minute), "check_suite", "requested", "ERROR", 500),
			webhookRedriveTestDelivery(202, windowStart.Add(-time.Minute), "pull_request", "opened", "ERROR", 500),
		}},
		details: map[int64]*gh.HookDelivery{
			201: {ID: new(int64(201)), Request: &gh.HookRequest{RawPayload: &raw}},
		},
	}

	result, err := redriveWebhookAppDeliveries(t.Context(), client, "default", "", windowStart, windowEnd, 10, true, "octo/repo", 12, discardLogger())

	require.NoError(t, err)
	require.Len(t, result.Selected, 1)
	assert.Equal(t, int64(201), result.Selected[0].ID)
	assert.Equal(t, 12, result.Selected[0].PR)
}

// When GitHub's retained delivery history ends before the requested window
// start, the result reports the fact so the caller can distinguish "raise the
// page budget" from "older deliveries no longer exist".
func TestRedriveWebhookAppDeliveriesReportsExhaustedHistory(t *testing.T) {
	t.Parallel()

	windowStart := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	windowEnd := time.Date(2026, 7, 7, 19, 50, 0, 0, time.UTC)
	client := &fakeWebhookRedriveDeliveryClient{
		pages: [][]*gh.HookDelivery{{
			webhookRedriveTestDelivery(301, windowEnd.Add(-time.Minute), "pull_request", "opened", "ERROR", 500),
		}},
	}

	result, err := redriveWebhookAppDeliveries(t.Context(), client, "default", "", windowStart, windowEnd, 10, true, "", 0, discardLogger())

	require.NoError(t, err)
	assert.False(t, result.ReachedWindowStart)
	assert.True(t, result.HistoryExhausted)
	assert.Empty(t, result.NextCursor)
}

// A failed delivery is not re-selected when a newer redelivery of it (same
// GUID) already succeeded during the crawl, so repeated redrives over a
// stable window converge instead of re-firing downstream events.
func TestRedriveWebhookAppDeliveriesSkipsAlreadySucceededRedeliveries(t *testing.T) {
	t.Parallel()

	windowStart := time.Date(2026, 7, 7, 19, 40, 0, 0, time.UTC)
	windowEnd := time.Date(2026, 7, 7, 19, 50, 0, 0, time.UTC)
	succeededRedelivery := webhookRedriveTestDelivery(11, windowEnd.Add(-time.Minute), "pull_request", "opened", "OK", 200)
	succeededRedelivery.GUID = new("guid-a")
	failedOriginalA := webhookRedriveTestDelivery(10, windowEnd.Add(-2*time.Minute), "pull_request", "opened", "ERROR", 500)
	failedOriginalA.GUID = new("guid-a")
	failedOriginalB := webhookRedriveTestDelivery(20, windowEnd.Add(-3*time.Minute), "pull_request", "opened", "ERROR", 500)
	failedOriginalB.GUID = new("guid-b")

	// Newest first: the successful redelivery of guid-a precedes its failed original.
	client := &fakeWebhookRedriveDeliveryClient{pages: [][]*gh.HookDelivery{{succeededRedelivery, failedOriginalA, failedOriginalB}}}

	result, err := redriveWebhookAppDeliveries(t.Context(), client, "default", "", windowStart, windowEnd, 10, true, "", 0, discardLogger())

	require.NoError(t, err)
	require.Len(t, result.Selected, 1, "guid-a already succeeded; only guid-b remains")
	assert.Equal(t, int64(20), result.Selected[0].ID)
}

// A per-delivery detail-fetch failure during repo/PR filtering skips that one
// delivery (counted) instead of aborting the whole crawl.
func TestRedriveWebhookAppDeliveriesSkipsDeliveriesWithUnresolvableDetail(t *testing.T) {
	t.Parallel()

	windowStart := time.Date(2026, 7, 7, 19, 40, 0, 0, time.UTC)
	windowEnd := time.Date(2026, 7, 7, 19, 50, 0, 0, time.UTC)
	client := &fakeWebhookRedriveDeliveryClient{
		pages: [][]*gh.HookDelivery{{
			webhookRedriveTestDelivery(101, windowEnd.Add(-time.Minute), "pull_request", "opened", "ERROR", 500),
			webhookRedriveTestDelivery(102, windowEnd.Add(-2*time.Minute), "pull_request", "opened", "ERROR", 500),
		}},
		details: map[int64]*gh.HookDelivery{
			101: webhookRedriveTestDeliveryDetail(101, "octo/repo", 12),
			// 102 has no detail → GetHookDelivery errors → skipped, not fatal.
		},
	}

	result, err := redriveWebhookAppDeliveries(t.Context(), client, "default", "", windowStart, windowEnd, 10, true, "octo/repo", 12, discardLogger())

	require.NoError(t, err)
	assert.Equal(t, 1, result.Skipped)
	require.Len(t, result.Selected, 1)
	assert.Equal(t, int64(101), result.Selected[0].ID)
}

func webhookRedriveTestDelivery(id int64, deliveredAt time.Time, event, action, status string, statusCode int) *gh.HookDelivery {
	return &gh.HookDelivery{
		ID:          new(id),
		DeliveredAt: &gh.Timestamp{Time: deliveredAt},
		Event:       new(event),
		Action:      new(action),
		Status:      new(status),
		StatusCode:  new(statusCode),
	}
}

func webhookRedriveTestDeliveryDetail(id int64, repo string, pr int) *gh.HookDelivery {
	payload, err := json.Marshal(map[string]any{
		"number": pr,
		"repository": map[string]any{
			"full_name": repo,
		},
	})
	if err != nil {
		panic(err)
	}
	raw := json.RawMessage(payload)
	return &gh.HookDelivery{
		ID: new(id),
		Request: &gh.HookRequest{
			RawPayload: &raw,
		},
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type fakeCheckRunBackfiller struct {
	calls    []int
	failPR   int
	outcomes map[int]string
}

func (f *fakeCheckRunBackfiller) BackfillPRCheckRuns(_ context.Context, _ string, pr int, _ int64) (string, error) {
	f.calls = append(f.calls, pr)
	if pr == f.failPR {
		return "", errors.New("boom")
	}
	if outcome, ok := f.outcomes[pr]; ok {
		return outcome, nil
	}
	return "auto-plan started", nil
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
	assert.Contains(t, err.Error(), "prs is required")

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
}

// A stale or mistyped environment is rejected as a request error before any
// GitHub work, rather than scanning for a check name that can never exist and
// reporting every PR as missing it.
func TestExecuteChecksScanRejectsDisallowedEnvironment(t *testing.T) {
	t.Parallel()

	cfg := &ServerConfig{AllowedEnvironments: []string{"staging", "production"}}

	_, err := executeChecksScan(t.Context(), cfg, ChecksScanRequest{Repo: "octo/repo", Environment: "prod"}, discardLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), `environment "prod" is not one this instance handles`)
}

// Redelivery by explicit delivery IDs is a precise continuation of a prior
// listing pass; it refuses request shapes that would silently change meaning.
func TestExecuteWebhookRedriveByIDsValidation(t *testing.T) {
	t.Parallel()

	cfg := &ServerConfig{GitHub: GitHubConfig{AppID: "1", PrivateKey: "key"}}

	_, err := executeWebhookRedrive(t.Context(), cfg, WebhookRedriveRequest{DeliveryIDs: []int64{1}}, discardLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires app")

	_, err = executeWebhookRedrive(t.Context(), cfg, WebhookRedriveRequest{DeliveryIDs: []int64{1}, App: "default", Cursor: "c1"}, discardLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mutually exclusive")

	_, err = executeWebhookRedrive(t.Context(), cfg, WebhookRedriveRequest{DeliveryIDs: []int64{1}, App: "default", DryRun: true}, discardLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no dry run")
}

type fakeWebhookMissingCheckScanClient struct {
	prs  []ghclient.OpenPullRequest
	runs map[string]*ghclient.CheckRunResult
	// untrusted maps headSHA+"/"+checkName to app slugs that published a
	// same-named check but are not trusted.
	untrusted map[string][]string
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

	result, err := scanWebhookMissingChecks(t.Context(), client, "octo/repo", []string{"SchemaBot (production)"}, 0, time.Time{})

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

	result, err := scanWebhookMissingChecks(t.Context(), client, "octo/repo", []string{"SchemaBot (production)"}, 0, time.Time{})

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

	result, err := scanWebhookMissingChecks(t.Context(), client, "octo/repo", []string{"SchemaBot (production)"}, 0, time.Time{})

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

	result, err := scanWebhookMissingChecks(t.Context(), client, "octo/repo", []string{"SchemaBot (production)"}, 0, updatedSince)

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

	_, err := executeChecksScan(t.Context(), cfg, ChecksScanRequest{Repo: "octo/repo", UpdatedSince: "yesterday"}, discardLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "updated_since")
}

// The repos inventory returns the declared repos config sorted, and a legacy
// single-App config (which declares no repos) is a request error telling the
// operator to name a repository — a fleet sweep cannot guess what to scan.
func TestExecuteChecksRepos(t *testing.T) {
	t.Parallel()

	cfg := &ServerConfig{
		Apps:  map[string]GitHubAppConfig{"main": {AppID: "1", PrivateKey: "key"}},
		Repos: map[string]RepoConfig{"octo/zebra": {GitHubApp: "main"}, "octo/alpha": {GitHubApp: "main"}},
	}
	response, err := executeChecksRepos(cfg)
	require.NoError(t, err)
	assert.Equal(t, []string{"octo/alpha", "octo/zebra"}, response.Repos)

	_, err = executeChecksRepos(&ServerConfig{GitHub: GitHubConfig{AppID: "1", PrivateKey: "key"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "declares no repos config")
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

	result, err := scanWebhookMissingChecks(t.Context(), client, "octo/repo", []string{"SchemaBot (production)"}, 0, time.Time{})

	require.NoError(t, err)
	require.Len(t, result.Missing, 1)
	assert.Equal(t, []string{"SchemaBot (production)"}, result.Missing[0].MissingNames)
	assert.Equal(t, []string{"SchemaBot (production)"}, result.Missing[0].UntrustedConflictNames)
}
