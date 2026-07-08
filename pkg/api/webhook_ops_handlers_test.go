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
