package commands

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
)

func TestParseOperatorDuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
		want  time.Duration
	}{
		{name: "go duration hours", value: "6h", want: 6 * time.Hour},
		{name: "go duration minutes", value: "90m", want: 90 * time.Minute},
		{name: "days suffix", value: "2d", want: 48 * time.Hour},
		{name: "days word", value: "2 days", want: 48 * time.Hour},
		{name: "hour word", value: "1 hour", want: time.Hour},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseOperatorDuration(tt.value)

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseOperatorDurationRejectsInvalidValues(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"", "0h", "-1h", "two days", "2 months"} {
		t.Run(value, func(t *testing.T) {
			t.Parallel()

			_, err := parseOperatorDuration(value)

			require.Error(t, err)
		})
	}
}

func TestRedriveWebhooksResolveWindowFromLast(t *testing.T) {
	fixedNow := time.Date(2026, 7, 8, 16, 0, 0, 0, time.UTC)
	originalNow := webhookRedriveNow
	webhookRedriveNow = func() time.Time { return fixedNow }
	t.Cleanup(func() { webhookRedriveNow = originalNow })

	cmd := RedriveWebhooksCmd{Last: "2h"}

	windowStart, windowEnd, err := cmd.resolveWindow()

	require.NoError(t, err)
	assert.Equal(t, "2026-07-08T14:00:00Z", windowStart)
	assert.Equal(t, "2026-07-08T16:00:00Z", windowEnd)
}

func TestRedriveWebhooksResolveWindowRejectsMixedShortcutAndExplicitWindow(t *testing.T) {
	t.Parallel()

	cmd := RedriveWebhooksCmd{WindowStart: "2026-07-08T14:00:00Z", WindowEnd: "2026-07-08T16:00:00Z", Last: "2h"}

	_, _, err := cmd.resolveWindow()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "either --last or explicit window start/end")
}

func TestRedriveWebhooksResolveWindowRequiresWindow(t *testing.T) {
	t.Parallel()

	cmd := RedriveWebhooksCmd{}

	_, _, err := cmd.resolveWindow()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "provide window start/end or use --last")
}

// A redrive that reports failed redeliveries must fail the command, so
// scripted redrives detect partial failure from the exit code.
// A run with failed redeliveries or skipped (detail-unresolved) eligible
// deliveries did not fully cover the window, so the command fails so a
// scripted caller detects the under-coverage; a clean run succeeds.
func TestWriteRedriveWebhookResponseFailsOnIncompleteCoverage(t *testing.T) {
	failed := writeRedriveWebhookResponse(&apitypes.WebhookRedriveResponse{
		Results: []apitypes.WebhookRedriveResult{
			{AppName: "default", Redelivered: 2, Failed: 1},
		},
	}, false)
	require.Error(t, failed)
	require.Contains(t, failed.Error(), "1 redeliveries failed")

	skipped := writeRedriveWebhookResponse(&apitypes.WebhookRedriveResponse{
		Results: []apitypes.WebhookRedriveResult{
			{AppName: "default", Redelivered: 2, Skipped: 3},
		},
	}, false)
	require.Error(t, skipped)
	require.Contains(t, skipped.Error(), "3 eligible deliveries skipped")

	require.NoError(t, writeRedriveWebhookResponse(&apitypes.WebhookRedriveResponse{
		Results: []apitypes.WebhookRedriveResult{
			{AppName: "default", Redelivered: 3},
		},
	}, false))
}

// The CLI covers a redrive window with a series of bounded server requests,
// following each app's cursor until the window start is reached, and merges
// the chunks into one per-app result.
func TestRunChunkedWebhookRedriveFollowsCursorsAndMerges(t *testing.T) {
	var requests []apitypes.WebhookRedriveRequest
	call := func(_ context.Context, _ string, req apitypes.WebhookRedriveRequest) (*apitypes.WebhookRedriveResponse, error) {
		requests = append(requests, req)
		if req.Cursor == "" {
			return &apitypes.WebhookRedriveResponse{Results: []apitypes.WebhookRedriveResult{
				{AppName: "default", Pages: 2, Fetched: 200, Redelivered: 1, NextCursor: "c1",
					Selected: []apitypes.WebhookRedriveSelection{{ID: 1}}},
			}}, nil
		}
		require.Equal(t, "c1", req.Cursor)
		require.Equal(t, "default", req.App)
		return &apitypes.WebhookRedriveResponse{Results: []apitypes.WebhookRedriveResult{
			{AppName: "default", Pages: 3, Fetched: 250, Redelivered: 2, ReachedWindowStart: true,
				Selected: []apitypes.WebhookRedriveSelection{{ID: 2}, {ID: 3}}},
		}}, nil
	}

	merged, err := runChunkedWebhookRedrive(t.Context(), call, "http://server", apitypes.WebhookRedriveRequest{
		WindowStart: "2026-07-07T19:40:00Z", WindowEnd: "2026-07-07T19:50:00Z", MaxPages: 10,
	}, 10, nil)

	require.NoError(t, err)
	require.Len(t, requests, 2)
	require.Len(t, merged.Results, 1)
	result := merged.Results[0]
	assert.Equal(t, 5, result.Pages)
	assert.Equal(t, 450, result.Fetched)
	assert.Equal(t, 3, result.Redelivered)
	assert.Len(t, result.Selected, 3)
	assert.True(t, result.ReachedWindowStart)
}

// Incomplete coverage fails the run with the remediation that actually
// helps: a bigger page budget when the cursor is still live, or the fact
// that GitHub no longer retains the older deliveries.
func TestRunChunkedWebhookRedriveFailsOnIncompleteCoverage(t *testing.T) {
	budgetExhausted := func(_ context.Context, _ string, _ apitypes.WebhookRedriveRequest) (*apitypes.WebhookRedriveResponse, error) {
		return &apitypes.WebhookRedriveResponse{Results: []apitypes.WebhookRedriveResult{
			{AppName: "default", Pages: 5, NextCursor: "c9"},
		}}, nil
	}
	_, err := runChunkedWebhookRedrive(t.Context(), budgetExhausted, "http://server", apitypes.WebhookRedriveRequest{
		WindowStart: "2026-07-07T19:40:00Z", WindowEnd: "2026-07-07T19:50:00Z", MaxPages: 5,
	}, 5, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "increase --max-pages")

	historyEnded := func(_ context.Context, _ string, _ apitypes.WebhookRedriveRequest) (*apitypes.WebhookRedriveResponse, error) {
		return &apitypes.WebhookRedriveResponse{Results: []apitypes.WebhookRedriveResult{
			{AppName: "default", Pages: 3, HistoryExhausted: true, OldestFetched: "2026-07-07T19:45:00Z"},
		}}, nil
	}
	_, err = runChunkedWebhookRedrive(t.Context(), historyEnded, "http://server", apitypes.WebhookRedriveRequest{
		WindowStart: "2026-07-07T19:40:00Z", WindowEnd: "2026-07-07T19:50:00Z", MaxPages: 5,
	}, 5, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "retained delivery history ends")
}

// The progress line reports how far back through the window the crawl has
// walked, from the newest deliveries toward the window start.
func TestRedriveWindowCoverage(t *testing.T) {
	pct, ok := redriveWindowCoverage("2026-07-07T19:45:00Z", "2026-07-07T19:40:00Z", "2026-07-07T19:50:00Z")
	require.True(t, ok)
	assert.Equal(t, 50, pct)

	pct, ok = redriveWindowCoverage("2026-07-07T19:39:00Z", "2026-07-07T19:40:00Z", "2026-07-07T19:50:00Z")
	require.True(t, ok)
	assert.Equal(t, 100, pct, "an oldest delivery before the window start caps at full coverage")

	_, ok = redriveWindowCoverage("", "2026-07-07T19:40:00Z", "2026-07-07T19:50:00Z")
	assert.False(t, ok, "no oldest timestamp yet")
}

// A cancelled context stops the chunked crawl and returns the work completed
// so far alongside context.Canceled, so the command can report a partial
// summary instead of losing it.
func TestRunChunkedWebhookRedriveReturnsPartialOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	calls := 0
	call := func(callCtx context.Context, _ string, _ apitypes.WebhookRedriveRequest) (*apitypes.WebhookRedriveResponse, error) {
		if callCtx.Err() != nil {
			return nil, callCtx.Err()
		}
		calls++
		// First chunk completes with redeliveries and more work pending; the
		// operator cancels before the continuation runs.
		cancel()
		return &apitypes.WebhookRedriveResponse{Results: []apitypes.WebhookRedriveResult{{
			AppName:     "production",
			Redelivered: 3,
			NextCursor:  "page-2",
		}}}, nil
	}

	merged, err := runChunkedWebhookRedrive(ctx, call, "http://server", apitypes.WebhookRedriveRequest{
		WindowStart: "2026-07-07T19:40:00Z",
		WindowEnd:   "2026-07-07T19:50:00Z",
		App:         "production",
	}, 10, nil)

	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, merged.Results, 1)
	assert.Equal(t, 3, merged.Results[0].Redelivered, "work completed before cancel is preserved")
}

// redriveWindowCmd redrives a fixed window for one App, so a test controls
// every request the command makes.
func redriveWindowCmd() *RedriveWebhooksCmd {
	return &RedriveWebhooksCmd{
		WindowStart: "2026-07-07T19:40:00Z",
		WindowEnd:   "2026-07-07T19:50:00Z",
		App:         "production",
		MaxPages:    10,
	}
}

// An operator presses Ctrl-C after the first chunk of a redrive has
// redelivered deliveries and while the next chunk is waiting on the server.
// The command says the redrive was canceled, names what it redelivered before
// the cancel, and exits with the interrupt status, so a script can tell an
// interrupted redrive from one that failed.
func TestRedriveWebhooksCancelExitsInterrupted(t *testing.T) {
	hold := newHoldingHandler(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/webhooks/redrive", r.URL.Path)
		// The server notices the client hanging up only once the body is read.
		body, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		var req apitypes.WebhookRedriveRequest
		assert.NoError(t, json.Unmarshal(body, &req))
		if req.Cursor != "" {
			hold.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		assert.NoError(t, json.NewEncoder(w).Encode(apitypes.WebhookRedriveResponse{Results: []apitypes.WebhookRedriveResult{{
			AppName:     "production",
			Pages:       1,
			Redelivered: 3,
			NextCursor:  "page-2",
		}}}))
	}))
	t.Cleanup(server.Close)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cmd := redriveWindowCmd()

	done := make(chan error, 1)
	var runErr error
	stderr := captureStderr(t, func() {
		go func() { done <- cmd.Run(ctx, &Globals{Endpoint: server.URL}) }()
		awaitSignal(t, hold.inFlight, "the second redrive chunk to reach the server")
		cancel()
		runErr = awaitSignal(t, done, "the canceled redrive to end")
	})

	require.ErrorIs(t, runErr, ErrSilent)
	require.ErrorIs(t, runErr, context.Canceled)
	assert.Equal(t, ExitInterrupted, ExitCodeFor(runErr))
	assert.Contains(t, stderr, "redrive canceled\napp=production redelivered=3 failed=0 (before cancel)\n")
}

// A redrive the server refuses is a failure, not an interrupt, and exits 1.
func TestRedriveWebhooksServerErrorExitsOne(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/webhooks/redrive", r.URL.Path)
		http.Error(w, "list deliveries failed", http.StatusBadGateway)
	}))
	t.Cleanup(server.Close)

	var runErr error
	captureStderr(t, func() {
		runErr = redriveWindowCmd().Run(t.Context(), &Globals{Endpoint: server.URL})
	})

	require.Error(t, runErr)
	assert.NotErrorIs(t, runErr, ErrSilent)
	assert.Contains(t, runErr.Error(), "list deliveries failed")
	assert.Equal(t, 1, ExitCodeFor(runErr))
}
