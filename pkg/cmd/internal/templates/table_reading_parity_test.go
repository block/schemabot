package templates

import (
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	webhooktemplates "github.com/block/schemabot/pkg/webhook/templates"
)

// inFlightCase is one table the engine is working on, as the progress API
// reports it.
type inFlightCase struct {
	name  string
	table apitypes.TableProgressResponse
	// metadata is the apply's engine metadata, such as its build work.
	metadata map[string]string
	// headline is what both surfaces show beside the table name.
	headline string
}

func inFlightCases() []inFlightCase {
	bytes := int64(3_400_000_000)
	running := func(t apitypes.TableProgressResponse) apitypes.TableProgressResponse {
		t.Status = state.Task.Running
		return t
	}
	return []inFlightCase{
		{name: "starting copy", table: running(apitypes.TableProgressResponse{ChangeType: "alter", RowsTotal: 7_200_000}), headline: "⏳ Starting copy..."},
		{name: "row copy", table: running(apitypes.TableProgressResponse{ChangeType: "alter", RowsCopied: 3_500_000, RowsTotal: 7_200_000, PercentComplete: 48, ETASeconds: 300, EstimatedBytes: &bytes}),
			headline: "48.61%"},
		{name: "throttled row copy", table: running(apitypes.TableProgressResponse{ChangeType: "alter", RowsCopied: 3_500_000, RowsTotal: 7_200_000, PercentComplete: 48, Throttled: true, ThrottleReason: "threads-running 41 over 32"}),
			headline: "48.61% (throttled)"},
		{name: "copy past its estimate", table: running(apitypes.TableProgressResponse{ChangeType: "alter", RowsCopied: 7_500_000, RowsTotal: 7_200_000, PercentComplete: 100}), headline: "Finalizing copy"},
		{name: "index build", table: running(apitypes.TableProgressResponse{ChangeType: "alter", PercentComplete: 25}),
			metadata: map[string]string{"executor_operation": "concurrent-index-build", "server_phase": "building index: scanning table", "blocks_done": "30000", "blocks_total": "48000"},
			headline: "25%"},
		{name: "create table", table: running(apitypes.TableProgressResponse{ChangeType: "create"}), headline: "Applying..."},
		{name: "instant alter", table: running(apitypes.TableProgressResponse{ChangeType: "alter", IsInstant: true}), headline: "Applying instantly..."},
		{name: "alter reporting no progress", table: running(apitypes.TableProgressResponse{ChangeType: "alter"}), headline: "Running..."},
		{name: "checksumming", table: apitypes.TableProgressResponse{ChangeType: "alter", Status: state.Task.Checksumming, RowsCopied: 7_200_000, RowsTotal: 7_200_000, ChecksumRowsChecked: 3_000_000, ChecksumRowsTotal: 7_200_000},
			headline: "🔍 Checksumming to verify data (41.67%)"},
		{name: "checksumming before a total", table: apitypes.TableProgressResponse{ChangeType: "alter", Status: state.Task.Checksumming, RowsCopied: 7_200_000, RowsTotal: 7_200_000},
			headline: "🔍 Checksumming to verify data..."},
		{name: "catching up", table: apitypes.TableProgressResponse{ChangeType: "alter", Status: state.Task.CatchingUp, RowsCopied: 7_200_000, RowsTotal: 7_200_000},
			headline: "⏩ Catching up on accumulated changes..."},
		{name: "applying final changes", table: apitypes.TableProgressResponse{ChangeType: "alter", Status: state.Task.PostChecksum, RowsCopied: 7_200_000, RowsTotal: 7_200_000},
			headline: "⏩ Data verified, applying final changes..."},
	}
}

// An operator following a schema change from the terminal and from the PR
// sees one table read the same way on both, whichever engine runs it: the
// same label beside the table name and the same detail lines under its
// statement, for every phase of the work in flight (UX-8). Only markup, bars
// drawn for the surface, and the docs link a PR note carries may differ.
func TestInFlightTableReadsTheSameOnEverySurface(t *testing.T) {
	for _, engine := range storage.Engines {
		for _, tc := range inFlightCases() {
			t.Run(engine+"/"+tc.name, func(t *testing.T) {
				table := tc.table
				table.TableName, table.DDL = "orders", "ALTER TABLE orders ADD INDEX idx_created (created_at)"
				resp := &apitypes.ProgressResponse{
					ApplyID: "apply-5e6f7a8b", Database: "shop", Environment: "staging",
					State: state.Apply.Running, Engine: engine, Metadata: tc.metadata,
					Tables: []*apitypes.TableProgressResponse{&table},
				}
				data := ParseProgressResponse(resp)
				require.Len(t, data.Tables, 1)
				cliHeadline, cliDetails := cliInFlightReading(t, FormatTableProgress(data.Tables[0]))
				prHeadline, prDetails := prInFlightReading(t, webhooktemplates.RenderApplyStatusComment(webhooktemplates.ApplyStatusFromProgress(resp, "octocat")))

				assert.Equal(t, tc.headline, withoutBar(cliHeadline), "CLI headline")
				assert.Equal(t, withoutBar(cliHeadline), withoutBar(prHeadline), "the PR comment's headline reads as the CLI's")
				assert.Equal(t, cliDetails, prDetails, "the PR comment's detail lines read as the CLI's")
			})
		}
	}
}

var (
	barGlyphs   = regexp.MustCompile(`^[🟦🟩🟨⬜⬛▓░█▒ ]+`)
	prDocsLink  = regexp.MustCompile(` \(\[docs\]\([^)]*\)\)`)
	cliTableRow = regexp.MustCompile(`^\s+\S*\s*orders: (.*)$`)
)

// withoutBar drops the progress bar that leads a headline, which each surface
// draws in its own glyphs, keeping the label.
func withoutBar(headline string) string {
	return barGlyphs.ReplaceAllString(headline, "")
}

// cliInFlightReading returns the CLI's headline for the table and the detail
// lines under its statement, without terminal styling.
func cliInFlightReading(t *testing.T, out string) (string, []string) {
	t.Helper()
	var headline string
	var details []string
	for line := range strings.SplitSeq(ansiEscape.ReplaceAllString(out, ""), "\n") {
		if m := cliTableRow.FindStringSubmatch(line); m != nil && headline == "" {
			headline = m[1]
			continue
		}
		if detail, ok := strings.CutPrefix(strings.TrimSpace(line), "• "); ok {
			details = append(details, detail)
		}
	}
	require.NotEmpty(t, headline, "CLI output has no line for the table:\n%s", out)
	return headline, details
}

// prInFlightReading returns the PR comment's headline for the table and the
// detail lines under its statement, without markdown emphasis or the docs
// link a note carries.
func prInFlightReading(t *testing.T, comment string) (string, []string) {
	t.Helper()
	const prefix = "**`orders`**: "
	start := strings.Index(comment, prefix)
	require.GreaterOrEqual(t, start, 0, "PR comment has no line for the table:\n%s", comment)
	rest := comment[start+len(prefix):]
	headline, rest, _ := strings.Cut(rest, "\n")
	// The detail lines follow the statement's closing fence.
	_, afterFence, ok := strings.Cut(rest, "\n```\n")
	require.True(t, ok, "PR comment has no closed statement block for the table:\n%s", comment)
	var details []string
	for line := range strings.SplitSeq(afterFence, "\n") {
		detail, ok := strings.CutPrefix(line, "- ")
		if !ok {
			break
		}
		detail = prDocsLink.ReplaceAllString(detail, "")
		if note, ok := strings.CutPrefix(detail, "ℹ️ _"); ok {
			detail = "ℹ️ " + strings.TrimSuffix(note, "_")
		}
		details = append(details, detail)
	}
	return headline, details
}
