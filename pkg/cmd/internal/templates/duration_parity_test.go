package templates

import (
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
	webhooktemplates "github.com/block/schemabot/pkg/webhook/templates"
)

// An operator watching one apply from the terminal and from the PR reads each
// duration the same way on both: the two largest non-zero units, with a zero
// unit left out (UX-5). An apply that ran 25 hours and 1 minute reads "1d 1h"
// on both, and a revert window with exactly 30 minutes left reads "30m" on
// both.
func TestDurationsReadTheSameOnEverySurface(t *testing.T) {
	now := time.Date(2026, 6, 16, 19, 42, 0, 0, time.UTC)
	prevNow, prevWebhookNow := nowFunc, webhooktemplates.NowFunc
	nowFunc = func() time.Time { return now }
	webhooktemplates.NowFunc = func() time.Time { return now }
	t.Cleanup(func() {
		nowFunc = prevNow
		webhooktemplates.NowFunc = prevWebhookNow
	})

	revertWindowAt := func(remaining time.Duration) map[string]string {
		return map[string]string{"revert_expires_at": now.Add(remaining).Format(time.RFC3339)}
	}
	// The PR comment shows an elapsed duration on the summary of an apply that
	// stopped, failed, or was cancelled, and the time left on a revert window
	// in the status line.
	for _, tc := range []struct {
		name      string
		applyDone bool
		resp      apitypes.ProgressResponse
		cliRow    string
		prLabel   string
		reading   string
	}{
		{
			name: "an apply that ran 25h 1m", applyDone: true, cliRow: "Duration", prLabel: "**Duration**: ", reading: "1d 1h",
			resp: apitypes.ProgressResponse{State: state.Apply.Failed, StartedAt: "2026-06-15T18:00:00Z", CompletedAt: "2026-06-16T19:01:00Z"},
		},
		{
			name: "an apply that ran 90s", applyDone: true, cliRow: "Duration", prLabel: "**Duration**: ", reading: "1m 30s",
			resp: apitypes.ProgressResponse{State: state.Apply.Failed, StartedAt: "2026-06-16T19:40:00Z", CompletedAt: "2026-06-16T19:41:30Z"},
		},
		{
			name: "an apply that ran exactly 30m", applyDone: true, cliRow: "Duration", prLabel: "**Duration**: ", reading: "30m",
			resp: apitypes.ProgressResponse{State: state.Apply.Failed, StartedAt: "2026-06-16T19:00:00Z", CompletedAt: "2026-06-16T19:30:00Z"},
		},
		{
			name: "a revert window with exactly 30m left", cliRow: "Revert expires in", prLabel: "Closes in ", reading: "30m",
			resp: apitypes.ProgressResponse{State: state.Apply.RevertWindow, Metadata: revertWindowAt(30 * time.Minute)},
		},
		{
			name: "a revert window with 30h left", cliRow: "Revert expires in", prLabel: "Closes in ", reading: "1d 6h",
			resp: apitypes.ProgressResponse{State: state.Apply.RevertWindow, Metadata: revertWindowAt(30 * time.Hour)},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			taskState := state.Task.RevertWindow
			if tc.applyDone {
				taskState = state.Task.Failed
			}
			resp := tc.resp
			resp.ApplyID, resp.Database, resp.Environment = "apply-5e6f7a8b", "shop", "staging"
			resp.Tables = []*apitypes.TableProgressResponse{{
				TableName: "orders", DDL: "ALTER TABLE orders ADD COLUMN note text", ChangeType: "alter", Status: taskState,
			}}

			cli := ansiEscape.ReplaceAllString(captureStdout(t, func() { WriteProgress(ParseProgressResponse(&resp)) }), "")
			data := webhooktemplates.ApplyStatusFromProgress(&resp, "octocat")
			pr := webhooktemplates.RenderApplyStatusComment(data)
			if tc.applyDone {
				pr = webhooktemplates.RenderApplySummaryComment(data)
			}

			// A reading ends where the next cell, border, or line starts, so
			// "1d 1h" does not match "1d 1h 1m".
			cliReading := regexp.MustCompile(`(?m)` + regexp.QuoteMeta(tc.cliRow) + `:?\s+` + regexp.QuoteMeta(tc.reading) + `\s*(│|$)`)
			prReading := regexp.MustCompile(`(?m)` + regexp.QuoteMeta(tc.prLabel+tc.reading) + `( \||\s*$)`)
			assert.Regexp(t, cliReading, cli, "the CLI reads the duration as %q", tc.reading)
			assert.Regexp(t, prReading, pr, "the PR comment reads the duration as %q", tc.reading)
		})
	}
}
