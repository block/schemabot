package templates

import (
	"fmt"
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

// shardedInFlightCase is a sharded table copying across its shards: the rows
// summed over the shards that have reported, and how many of the table's
// shards those are.
type shardedInFlightCase struct {
	name              string
	rowsCopied        int64
	rowsTotal         int64
	reporting, shards int
	headline          string
}

// A sharded table copying across its shards reads the same in the terminal
// and in the PR comment: the same percentage beside the table name, the same
// coverage while later shards have not reported, and the same rows line
// (UX-8). Only the shard list under it is laid out per surface.
func TestShardedInFlightTableReadsTheSameOnEverySurface(t *testing.T) {
	for _, tc := range []shardedInFlightCase{
		{name: "every shard reporting", rowsCopied: 914_707, rowsTotal: 1_466_232, reporting: 2, shards: 2, headline: "62.38%"},
		{name: "later shards not reporting", rowsCopied: 914_707, rowsTotal: 1_466_232, reporting: 1, shards: 4, headline: "62.38% (1 of 4 shards)"},
		{name: "copy past its estimate", rowsCopied: 1_600_000, rowsTotal: 1_466_232, reporting: 1, shards: 4, headline: "Finalizing copy (1 of 4 shards)"},
		{name: "starting copy", rowsTotal: 1_466_232, reporting: 2, shards: 2, headline: "⏳ Starting copy..."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cliShards := make([]ShardProgress, tc.shards)
			prShards := make([]webhooktemplates.ShardProgressData, tc.shards)
			for i := range tc.shards {
				name := fmt.Sprintf("shard-%d", i)
				cliShards[i] = ShardProgress{Shard: name, Status: state.Task.Pending}
				prShards[i] = webhooktemplates.ShardProgressData{Shard: name, Status: state.Task.Pending}
				if i == 0 {
					// The first shard carries the reporting shards' rows, so
					// the sums both surfaces read are the case's.
					cliShards[i] = ShardProgress{Shard: name, Status: state.Task.Running, RowsCopied: tc.rowsCopied, RowsTotal: tc.rowsTotal}
					prShards[i] = webhooktemplates.ShardProgressData{Shard: name, Status: state.Task.Running, RowsCopied: tc.rowsCopied, RowsTotal: tc.rowsTotal}
				} else if i < tc.reporting {
					// A reporting shard that has copied nothing yet still
					// counts toward the coverage.
					cliShards[i] = ShardProgress{Shard: name, Status: state.Task.Running, RowsTotal: 1}
					prShards[i] = webhooktemplates.ShardProgressData{Shard: name, Status: state.Task.Running, RowsTotal: 1}
				}
			}
			total := tc.rowsTotal + int64(tc.reporting-1)
			cli := FormatTableProgress(TableProgress{
				TableName: "orders", Status: state.Task.Running, ChangeType: "alter",
				RowsCopied: tc.rowsCopied, RowsTotal: total, ETASeconds: 195, PlannedShards: tc.shards, Shards: cliShards,
			})
			pr := webhooktemplates.RenderShardedApplyComment(webhooktemplates.ShardedApplyData{
				State: state.Apply.Running, Environment: "staging", Database: "shop", ApplyID: "apply-5e6f7a8b",
				Keyspaces: []webhooktemplates.ShardedKeyspace{{Keyspace: "shop_sharded", Tables: []webhooktemplates.ShardedTableStatus{{
					Table: "orders", Status: state.Task.Running, RowsCopied: tc.rowsCopied, RowsTotal: total, ETASeconds: 195,
					ShardsReporting: tc.reporting, PlannedShards: tc.shards, Shards: prShards,
				}}}},
			})

			// The same table in a single-deployment apply, as the progress
			// API reports it, reaches the apply status comment.
			apiShards := make([]*apitypes.ShardProgressResponse, tc.shards)
			for i, sh := range cliShards {
				apiShards[i] = &apitypes.ShardProgressResponse{Shard: sh.Shard, Status: sh.Status, RowsCopied: sh.RowsCopied, RowsTotal: sh.RowsTotal}
			}
			status := webhooktemplates.RenderApplyStatusComment(webhooktemplates.ApplyStatusFromProgress(&apitypes.ProgressResponse{
				ApplyID: "apply-5e6f7a8b", Database: "shop", Environment: "staging", State: state.Apply.Running,
				Tables: []*apitypes.TableProgressResponse{{
					TableName: "orders", DDL: "ALTER TABLE orders ADD COLUMN note text", Status: state.Task.Running, ChangeType: "alter",
					RowsCopied: tc.rowsCopied, RowsTotal: total, ETASeconds: 195, PlannedShards: int32(tc.shards), Shards: apiShards,
				}},
			}, "octocat"))

			cliHeadline, cliDetails := cliInFlightReading(t, cli)
			prHeadline, prDetails := shardedPRInFlightReading(t, pr)
			statusHeadline, statusDetails := shardedPRInFlightReading(t, status)
			assert.Equal(t, tc.headline, withoutBar(cliHeadline), "CLI headline")
			assert.Equal(t, withoutBar(cliHeadline), withoutBar(prHeadline), "the sharded PR comment's headline reads as the CLI's")
			assert.Equal(t, withoutBar(cliHeadline), withoutBar(statusHeadline), "the apply status comment's headline reads as the CLI's")
			assert.Equal(t, withoutShardList(cliDetails), withoutShardList(prDetails), "the sharded PR comment's detail lines read as the CLI's")
			assert.Equal(t, withoutShardList(cliDetails), withoutShardList(statusDetails), "the apply status comment's detail lines read as the CLI's")
			require.NotEmpty(t, withoutShardList(prDetails), "the table has a rows line")
		})
	}
}

// shardedPRInFlightReading returns the sharded PR comment's headline for the
// table and the detail lines under it, which follow the statement when the
// comment shows one.
func shardedPRInFlightReading(t *testing.T, comment string) (string, []string) {
	t.Helper()
	const prefix = "**`orders`**: "
	start := strings.Index(comment, prefix)
	require.GreaterOrEqual(t, start, 0, "PR comment has no line for the table:\n%s", comment)
	headline, rest, _ := strings.Cut(comment[start+len(prefix):], "\n")
	if _, afterFence, ok := strings.Cut(rest, "\n```\n"); ok {
		rest = afterFence
	}
	var details []string
	for line := range strings.SplitSeq(rest, "\n") {
		if line == "" {
			continue
		}
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

// withoutShardList drops the shard list that follows a sharded table's detail
// lines, which each surface lays out its own way.
func withoutShardList(details []string) []string {
	for i, d := range details {
		if strings.HasPrefix(d, "Shards:") {
			return details[:i]
		}
	}
	return details
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
