package commands

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	cmdclient "github.com/block/schemabot/pkg/cmd/client"
)

var webhookRedriveNow = func() time.Time { return time.Now().UTC() }

// WebhooksCmd groups GitHub webhook delivery operator commands. Missing
// Check Runs are handled by `schemabot checks backfill`, which redrives or
// synthesizes per PR; redrive here is the window-scoped tool for a known
// delivery outage.
type WebhooksCmd struct {
	Redrive RedriveWebhooksCmd `cmd:"" help:"Redeliver failed GitHub App webhook deliveries from a time window"`
}

// RedriveWebhooksCmd redelivers failed GitHub App webhook deliveries from a time window.
type RedriveWebhooksCmd struct {
	WindowStart string `arg:"" optional:"" help:"Inclusive RFC3339 window start, for example 2026-07-07T19:40:00Z"`
	WindowEnd   string `arg:"" optional:"" help:"Inclusive RFC3339 window end, for example 2026-07-07T19:50:00Z"`
	Last        string `help:"Convenience window ending now, for example 1h, 6h, 2d, or '2 days'"`
	App         string `help:"Configured GitHub App name to redrive; defaults to all configured Apps"`
	Repo        string `help:"Only redrive deliveries for this repository, in owner/name form"`
	PR          int    `help:"Only redrive deliveries for this pull request number"`
	MaxPages    int    `help:"Maximum delivery pages to scan per App" default:"300"`
	DryRun      bool   `help:"Print selected delivery IDs without redelivering"`
}

func (cmd *RedriveWebhooksCmd) Run(ctx context.Context, g *Globals) error {
	if cmd.MaxPages <= 0 {
		return fmt.Errorf("--max-pages must be positive")
	}
	windowStart, windowEnd, err := cmd.resolveWindow()
	if err != nil {
		return err
	}
	if cmd.PR < 0 {
		return fmt.Errorf("--pr must be non-negative; 0 disables PR filtering")
	}
	endpoint, err := g.Resolve()
	if err != nil {
		return err
	}
	updateProgress, stopProgress := startLiveProgress(true)
	defer stopProgress()
	updateProgress("listing GitHub App webhook deliveries...")
	merged, err := runChunkedWebhookRedrive(ctx, cmdclient.RedriveWebhooks, endpoint, apitypes.WebhookRedriveRequest{
		WindowStart: windowStart,
		WindowEnd:   windowEnd,
		App:         cmd.App,
		Repo:        cmd.Repo,
		PR:          cmd.PR,
		MaxPages:    cmd.MaxPages,
		DryRun:      cmd.DryRun,
	}, cmd.MaxPages, true, func(r apitypes.WebhookRedriveResult) {
		updateProgress(redriveProgressLine(r, windowStart, windowEnd, cmd.DryRun))
	})
	stopProgress()
	if errors.Is(err, context.Canceled) {
		return reportWebhookRedriveCanceled(merged, cmd.DryRun)
	}
	if err != nil {
		return err
	}
	return writeRedriveWebhookResponse(merged, cmd.DryRun)
}

// reportWebhookRedriveCanceled prints what the redrive completed before the
// operator interrupted it, then returns ErrSilent so the CLI exits non-zero
// without a raw "context canceled" error line.
func reportWebhookRedriveCanceled(partial *apitypes.WebhookRedriveResponse, dryRun bool) error {
	fmt.Fprintln(os.Stderr, "redrive canceled")
	if partial != nil && !dryRun {
		for _, result := range partial.Results {
			fmt.Fprintf(os.Stderr, "app=%s redelivered=%d failed=%d (before cancel)\n", result.AppName, result.Redelivered, result.Failed)
		}
	}
	return ErrSilent
}

// redriveProgressLine summarizes one app's crawl so far: how far back through
// the window the listing has reached, and what has been selected/redelivered.
func redriveProgressLine(r apitypes.WebhookRedriveResult, windowStart, windowEnd string, dryRun bool) string {
	line := fmt.Sprintf("app %s: %d pages, %d deliveries scanned", r.AppName, r.Pages, r.Fetched)
	if pct, ok := redriveWindowCoverage(r.OldestFetched, windowStart, windowEnd); ok && !r.ReachedWindowStart {
		line += fmt.Sprintf(" (%d%% of window)", pct)
	}
	if dryRun {
		return fmt.Sprintf("%s, %d selected (dry run)", line, len(r.Selected))
	}
	line += fmt.Sprintf(", %d selected, %d redelivered", len(r.Selected), r.Redelivered)
	if r.Failed > 0 {
		line += fmt.Sprintf(", %d failed", r.Failed)
	}
	if r.Skipped > 0 {
		line += fmt.Sprintf(", %d skipped", r.Skipped)
	}
	return line
}

// redriveWindowCoverage reports how much of the window the crawl has walked
// back through, from the newest deliveries toward the window start.
func redriveWindowCoverage(oldestFetched, windowStart, windowEnd string) (int, bool) {
	oldest, err := time.Parse(time.RFC3339, oldestFetched)
	if err != nil {
		return 0, false
	}
	start, err := time.Parse(time.RFC3339, windowStart)
	if err != nil {
		return 0, false
	}
	end, err := time.Parse(time.RFC3339, windowEnd)
	if err != nil {
		return 0, false
	}
	total := end.Sub(start)
	if total <= 0 {
		return 0, false
	}
	covered := min(max(end.Sub(oldest), 0), total)
	// Compute the percentage in float64: covered*100 as a Duration (int64
	// nanoseconds) would overflow for windows longer than a few years.
	return int(float64(covered) / float64(total) * 100), true
}

// runChunkedWebhookRedrive covers the window with a series of bounded server
// requests: the server processes a few delivery pages per request and hands
// back a cursor, so no single HTTP request can outlive an intermediary
// timeout no matter how deep the crawl goes. maxPages is the total page
// budget per App across all requests. With requireFullCoverage, incomplete
// coverage fails the run; without it, the caller inspects the per-app
// ReachedWindowStart/HistoryExhausted facts (best-effort classification
// passes tolerate a partial crawl).
//
// Re-run convergence caveat: the server dedupes already-succeeded redeliveries
// by GUID, but only within a single request (one cursor chunk). Because this
// loop spreads a large window across independent requests, a re-run over a
// window big enough that a delivery's successful-redelivery record and its
// failed original fall in different chunks can redeliver that original again.
// This is safe — a re-redelivered event only re-enters the auto-plan flow,
// whose check recompute is idempotent and fail-closed and which never triggers
// an apply; the worst case is a duplicate plan comment. Running --dry-run first
// is the operator mitigation.
func runChunkedWebhookRedrive(ctx context.Context, call func(context.Context, string, apitypes.WebhookRedriveRequest) (*apitypes.WebhookRedriveResponse, error), endpoint string, req apitypes.WebhookRedriveRequest, maxPages int, requireFullCoverage bool, progress func(apitypes.WebhookRedriveResult)) (*apitypes.WebhookRedriveResponse, error) {
	if progress == nil {
		progress = func(apitypes.WebhookRedriveResult) {}
	}
	// merged accumulates completed work so a cancellation mid-crawl can still
	// report what was redelivered before the operator interrupted.
	merged := &apitypes.WebhookRedriveResponse{}
	first, err := call(ctx, endpoint, req)
	if err != nil {
		return merged, err
	}
	for i, result := range first.Results {
		appResult := result
		progress(appResult)
		for appResult.NextCursor != "" && appResult.Pages < maxPages {
			next, err := call(ctx, endpoint, apitypes.WebhookRedriveRequest{
				WindowStart: req.WindowStart,
				WindowEnd:   req.WindowEnd,
				App:         appResult.AppName,
				Repo:        req.Repo,
				PR:          req.PR,
				MaxPages:    maxPages - appResult.Pages,
				DryRun:      req.DryRun,
				Cursor:      appResult.NextCursor,
			})
			if err != nil {
				// Keep every app's work completed so far — this app's partial
				// plus the untouched first-chunk results of the apps not yet
				// reached (their first request already redelivered) — then
				// surface the error (a cancellation is reported by Run).
				merged.Results = append(merged.Results, appResult)
				merged.Results = append(merged.Results, first.Results[i+1:]...)
				return merged, fmt.Errorf("continue webhook redrive for app %q: %w", appResult.AppName, err)
			}
			if len(next.Results) != 1 {
				return merged, fmt.Errorf("continue webhook redrive for app %q: expected one result, got %d", appResult.AppName, len(next.Results))
			}
			appResult = mergeWebhookRedriveResults(appResult, next.Results[0])
			progress(appResult)
		}
		if requireFullCoverage && !appResult.ReachedWindowStart {
			if appResult.HistoryExhausted {
				return merged, fmt.Errorf("app %q: GitHub's retained delivery history ends at %s, after the requested window start %s; older deliveries are no longer redriveable", appResult.AppName, appResult.OldestFetched, req.WindowStart)
			}
			return merged, fmt.Errorf("app %q: window start %s not reached within %d pages; increase --max-pages", appResult.AppName, req.WindowStart, maxPages)
		}
		merged.Results = append(merged.Results, appResult)
	}
	return merged, nil
}

func mergeWebhookRedriveResults(base, next apitypes.WebhookRedriveResult) apitypes.WebhookRedriveResult {
	base.Fetched += next.Fetched
	base.Pages += next.Pages
	base.Selected = append(base.Selected, next.Selected...)
	base.Skipped += next.Skipped
	base.Redelivered += next.Redelivered
	base.Failed += next.Failed
	base.OldestFetched = next.OldestFetched
	base.ReachedWindowStart = next.ReachedWindowStart
	base.HistoryExhausted = next.HistoryExhausted
	base.NextCursor = next.NextCursor
	return base
}

func (cmd *RedriveWebhooksCmd) resolveWindow() (string, string, error) {
	if cmd.Last != "" {
		if cmd.WindowStart != "" || cmd.WindowEnd != "" {
			return "", "", fmt.Errorf("use either --last or explicit window start/end, not both")
		}
		duration, err := parseWebhookRedriveLast(cmd.Last)
		if err != nil {
			return "", "", err
		}
		windowEnd := webhookRedriveNow()
		windowStart := windowEnd.Add(-duration)
		return windowStart.Format(time.RFC3339), windowEnd.Format(time.RFC3339), nil
	}
	if cmd.WindowStart == "" || cmd.WindowEnd == "" {
		return "", "", fmt.Errorf("provide window start/end or use --last, for example --last 1h")
	}
	windowStart, err := time.Parse(time.RFC3339, cmd.WindowStart)
	if err != nil {
		return "", "", fmt.Errorf("parse window start %q as RFC3339: %w", cmd.WindowStart, err)
	}
	windowEnd, err := time.Parse(time.RFC3339, cmd.WindowEnd)
	if err != nil {
		return "", "", fmt.Errorf("parse window end %q as RFC3339: %w", cmd.WindowEnd, err)
	}
	if windowEnd.Before(windowStart) {
		return "", "", fmt.Errorf("window end must be at or after window start")
	}
	return cmd.WindowStart, cmd.WindowEnd, nil
}

func parseWebhookRedriveLast(value string) (time.Duration, error) {
	trimmed := strings.TrimSpace(strings.ToLower(value))
	if trimmed == "" {
		return 0, fmt.Errorf("--last must not be empty")
	}
	if duration, err := time.ParseDuration(trimmed); err == nil {
		if duration <= 0 {
			return 0, fmt.Errorf("--last must be positive")
		}
		return duration, nil
	}

	amountText, unit, ok := strings.Cut(trimmed, " ")
	if !ok {
		amountText, unit = splitWebhookRedriveLastAmountAndUnit(trimmed)
	}
	amount, err := strconv.Atoi(amountText)
	if err != nil {
		return 0, fmt.Errorf("parse --last %q: use a positive duration like 1h, 6h, 2d, or '2 days'", value)
	}
	if amount <= 0 {
		return 0, fmt.Errorf("--last must be positive")
	}
	switch strings.TrimSpace(unit) {
	case "d", "day", "days":
		return time.Duration(amount) * 24 * time.Hour, nil
	case "h", "hour", "hours":
		return time.Duration(amount) * time.Hour, nil
	case "m", "min", "mins", "minute", "minutes":
		return time.Duration(amount) * time.Minute, nil
	}
	return 0, fmt.Errorf("parse --last %q: use a positive duration like 1h, 6h, 2d, or '2 days'", value)
}

func splitWebhookRedriveLastAmountAndUnit(value string) (string, string) {
	for i, r := range value {
		if r < '0' || r > '9' {
			return value[:i], value[i:]
		}
	}
	return value, ""
}

func writeRedriveWebhookResponse(response *apitypes.WebhookRedriveResponse, dryRun bool) error {
	if response == nil {
		return nil
	}
	totalFailed := 0
	totalSkipped := 0
	for _, result := range response.Results {
		totalFailed += result.Failed
		totalSkipped += result.Skipped
		// The per-delivery detail is the dry-run's selection preview and can be
		// very large on a wide window, so print it only for --dry-run; a real
		// redrive reports the per-app redelivered/failed summary below.
		if dryRun {
			for _, selected := range result.Selected {
				if _, err := fmt.Fprintf(os.Stderr, "selected app=%s id=%d delivered_at=%s event=%s action=%s status=%s status_code=%d repo=%s pr=%d\n",
					result.AppName, selected.ID, selected.DeliveredAt, selected.Event, selected.Action, selected.Status, selected.StatusCode, selected.Repo, selected.PR); err != nil {
					return err
				}
				if _, err := fmt.Fprintf(os.Stdout, "%d\n", selected.ID); err != nil {
					return err
				}
			}
		}
		var err error
		if dryRun {
			_, err = fmt.Fprintf(os.Stderr, "app=%s dry_run_selected=%d skipped=%d\n", result.AppName, len(result.Selected), result.Skipped)
		} else {
			_, err = fmt.Fprintf(os.Stdout, "app=%s redelivered=%d failed=%d skipped=%d\n", result.AppName, result.Redelivered, result.Failed, result.Skipped)
		}
		if err != nil {
			return err
		}
	}
	// A skipped delivery is an in-window eligible delivery whose detail could
	// not be resolved for filtering — it may have needed redriving but was
	// dropped, so the window is not fully covered. Fail like a redelivery
	// failure so a scripted run detects the under-coverage and can re-run.
	if totalFailed > 0 || totalSkipped > 0 {
		return fmt.Errorf("webhook redrive incomplete: %d redeliveries failed, %d eligible deliveries skipped (detail unresolved); see the per-app counts above and the server logs", totalFailed, totalSkipped)
	}
	return nil
}
