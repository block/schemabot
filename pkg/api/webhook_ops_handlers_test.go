package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/url"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/checkstate"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
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
		// The redrive crawl takes the repository as an optional filter rather
		// than as its subject, so a malformed one matches no delivery instead
		// of failing anything: the crawl walks the whole window and answers
		// 200 with nothing selected, which reads exactly like a window that
		// really is empty.
		{"redrive", func(ctx context.Context, repo string) error {
			redriveCfg := &ServerConfig{GitHub: GitHubConfig{AppID: "1", PrivateKey: "key"}}
			_, err := executeWebhookRedrive(ctx, redriveCfg, WebhookRedriveRequest{
				Repo:        repo,
				MaxPages:    1,
				WindowStart: "2026-01-01T00:00:00Z",
				WindowEnd:   "2026-01-02T00:00:00Z",
			}, discardLogger())
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

	// The crawl's filters have nothing to narrow once the deliveries are named
	// outright, and this path never reads them. Honored silently, a repository
	// the crawl refuses outright would redeliver every named delivery anyway.
	_, err = executeWebhookRedrive(t.Context(), cfg, WebhookRedriveRequest{DeliveryIDs: []int64{1}, App: "default", Repo: "acme"}, discardLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nothing to narrow")

	_, err = executeWebhookRedrive(t.Context(), cfg, WebhookRedriveRequest{DeliveryIDs: []int64{1}, App: "default", PR: 412}, discardLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nothing to narrow")
}

// The redrive repository is an optional filter: an incident redrive replays a
// whole window across every repository, and passing no repository is how that
// is asked for. A guard on the filter's shape must not turn into a requirement.
func TestExecuteWebhookRedriveLeavesTheRepositoryOptional(t *testing.T) {
	t.Parallel()

	cfg := &ServerConfig{GitHub: GitHubConfig{AppID: "1", PrivateKey: "key"}}

	// The crawl gets as far as needing a GitHub client it cannot build from
	// this key, so what is pinned is the refusal that does not happen first.
	_, err := executeWebhookRedrive(t.Context(), cfg, WebhookRedriveRequest{
		MaxPages:    1,
		WindowStart: "2026-01-01T00:00:00Z",
		WindowEnd:   "2026-01-02T00:00:00Z",
	}, discardLogger())

	require.Error(t, err)
	assert.NotContains(t, err.Error(), "repo is required")
	assert.NotContains(t, err.Error(), "owner/name pair")
}

// A repository carrying surrounding whitespace clears the owner/name shape
// check — the padding lands inside the name half — and is then compared
// verbatim against the repository each delivery names, matching nothing. The
// crawl would walk the whole window and answer 200 with nothing selected, which
// reads exactly like a window that really is empty.
func TestExecuteWebhookRedriveTrimsTheRepositoryFilter(t *testing.T) {
	t.Parallel()

	windowStart := time.Date(2026, 7, 7, 19, 40, 0, 0, time.UTC)
	windowEnd := time.Date(2026, 7, 7, 19, 50, 0, 0, time.UTC)
	newClient := func() *fakeWebhookRedriveDeliveryClient {
		return &fakeWebhookRedriveDeliveryClient{
			pages:   [][]*gh.HookDelivery{{webhookRedriveTestDelivery(101, windowEnd.Add(-time.Minute), "pull_request", "opened", "ERROR", 500)}},
			details: map[int64]*gh.HookDelivery{101: webhookRedriveTestDeliveryDetail(101, "acme/store", 412)},
		}
	}
	selectWith := func(t *testing.T, repo string) []WebhookRedriveSelection {
		t.Helper()
		result, err := redriveWebhookAppDeliveries(t.Context(), newClient(), "default", "", windowStart, windowEnd, 10, true, repo, 0, discardLogger())
		require.NoError(t, err)
		return result.Selected
	}

	assert.Empty(t, selectWith(t, " acme/store "),
		"the filter is compared verbatim, so the padding is what makes it match nothing")
	require.Len(t, selectWith(t, canonicalRepo(" acme/store ")), 1,
		"trimmed, it names the repository the delivery names")
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
