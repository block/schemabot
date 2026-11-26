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
	// details, when set, are the detail lines both surfaces show under the
	// table's statement.
	details []string
	// noBar marks a table with no measured progress, beside whose name
	// neither surface draws a bar.
	noBar bool
}

func inFlightCases() []inFlightCase {
	bytes := int64(3_400_000_000)
	rows := int64(7_200_000)
	running := func(t apitypes.TableProgressResponse) apitypes.TableProgressResponse {
		t.Status = state.Task.Running
		return t
	}
	return []inFlightCase{
		{name: "starting copy", table: running(apitypes.TableProgressResponse{ChangeType: "alter", RowsTotal: 7_200_000}), headline: "⏳ Starting copy...", noBar: true},
		{name: "percent reported before any rows", table: running(apitypes.TableProgressResponse{ChangeType: "alter", RowsTotal: 7_200_000, PercentComplete: 12}),
			headline: "⏳ Starting copy...", noBar: true},
		{name: "row copy", table: running(apitypes.TableProgressResponse{ChangeType: "alter", RowsCopied: 3_500_000, RowsTotal: 7_200_000, PercentComplete: 48, ETASeconds: 300, EstimatedBytes: &bytes}),
			headline: "48.61%"},
		{name: "throttled row copy", table: running(apitypes.TableProgressResponse{ChangeType: "alter", RowsCopied: 3_500_000, RowsTotal: 7_200_000, PercentComplete: 48, Throttled: true, ThrottleReason: "threads-running 41 over 32"}),
			headline: "48.61% (throttled)"},
		{name: "copy past its estimate", table: running(apitypes.TableProgressResponse{ChangeType: "alter", RowsCopied: 7_500_000, RowsTotal: 7_200_000, PercentComplete: 100}), headline: "Finalizing copy"},
		{name: "copy finished short of its estimate", table: running(apitypes.TableProgressResponse{ChangeType: "alter", RowsCopied: 1, RowsTotal: 200_000, PercentComplete: 100}),
			headline: "100.00%", details: []string{"Rows copied: 1"}},
		{name: "copy finished with no rows copied", table: running(apitypes.TableProgressResponse{ChangeType: "alter", RowsTotal: 200_000, PercentComplete: 100}),
			headline: "100.00%"},
		{name: "index build", table: running(apitypes.TableProgressResponse{ChangeType: "alter", PercentComplete: 25}),
			metadata: map[string]string{"executor_operation": "concurrent-index-build", "server_phase": "building index: scanning table", "blocks_done": "30000", "blocks_total": "48000"},
			headline: "25%"},
		{name: "index build on a sized table", table: running(apitypes.TableProgressResponse{ChangeType: "alter", PercentComplete: 25, EstimatedRows: &rows, EstimatedBytes: &bytes}),
			metadata: map[string]string{"executor_operation": "concurrent-index-build", "server_phase": "building index: scanning table", "blocks_done": "30000", "blocks_total": "48000"},
			headline: "25%", details: []string{"Building index: 30,000 of 48,000 blocks", "Rows: ~7.2M · ~3.4 GB"}},
		{name: "index build on a sized table with no row count", table: running(apitypes.TableProgressResponse{ChangeType: "alter", PercentComplete: 25, EstimatedBytes: &bytes}),
			metadata: map[string]string{"executor_operation": "concurrent-index-build", "server_phase": "building index: scanning table", "blocks_done": "30000", "blocks_total": "48000"},
			headline: "25%", details: []string{"Building index: 30,000 of 48,000 blocks", "Size: ~3.4 GB"}},
		{name: "create table", table: running(apitypes.TableProgressResponse{ChangeType: "create"}), headline: "Applying...", noBar: true},
		{name: "instant alter", table: running(apitypes.TableProgressResponse{ChangeType: "alter", IsInstant: true}), headline: "Applying instantly...", noBar: true},
		{name: "alter reporting no progress", table: running(apitypes.TableProgressResponse{ChangeType: "alter"}), headline: "Running...", noBar: true},
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
				if tc.details != nil {
					assert.Equal(t, tc.details, cliDetails, "CLI detail lines")
				}
				assert.Equal(t, !tc.noBar, hasBar(cliHeadline), "CLI bar")
				assert.Equal(t, !tc.noBar, hasBar(prHeadline), "PR comment bar")
			})
		}
	}
}

// One table's copy finishes short of its estimate while another table is
// still copying, so the finished table's task stays running. The finished copy
// may have inserted no rows at all, when the source emptied after its estimate
// or the binlog applier moved every row first. Both surfaces read it as fully
// copied, with the rows it copied, and never as a percent of the estimate
// beside a full bar or as a copy that is starting. The copying table keeps its
// own percent (UX-8).
func TestFinishedCopyReadsAsCopiedBesideATableStillCopying(t *testing.T) {
	for _, tc := range []struct {
		name       string
		rowsCopied int64
		rowsLine   string
	}{
		{name: "some rows copied", rowsCopied: 1, rowsLine: "Rows copied: 1"},
		{name: "no rows copied", rowsCopied: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := &apitypes.ProgressResponse{
				ApplyID: "apply-5e6f7a8b", Database: "shop", Environment: "staging",
				State: state.Apply.Running, Engine: storage.EngineSpirit,
				Tables: []*apitypes.TableProgressResponse{
					{TableName: "events", DDL: "ALTER TABLE events ADD UNIQUE KEY uk_email (email)", ChangeType: "alter",
						Status: state.Task.Running, RowsCopied: tc.rowsCopied, RowsTotal: 200_000, PercentComplete: 100},
					{TableName: "orders", DDL: "ALTER TABLE orders ADD INDEX idx_created (created_at)", ChangeType: "alter",
						Status: state.Task.Running, RowsCopied: 3_500_000, RowsTotal: 7_200_000, PercentComplete: 48},
				},
			}
			data := ParseProgressResponse(resp)
			require.Len(t, data.Tables, 2)
			cli := ansiEscape.ReplaceAllString(FormatTableProgress(data.Tables[0])+FormatTableProgress(data.Tables[1]), "")
			pr := webhooktemplates.RenderApplyStatusComment(webhooktemplates.ApplyStatusFromProgress(resp, "octocat"))

			for surface, out := range map[string]string{"CLI": cli, "PR comment": pr} {
				assert.Contains(t, out, "100.00%", "%s: the finished table reads as fully copied", surface)
				if tc.rowsLine != "" {
					assert.Contains(t, out, tc.rowsLine, "%s: the finished table shows the rows it copied", surface)
				}
				assert.NotContains(t, out, "Starting copy", "%s: the finished table reads as a copy that is starting", surface)
				assert.NotContains(t, out, "0.01%", "%s: the finished table reads as a percent of its estimate", surface)
				assert.NotContains(t, out, "/ 200,000", "%s: the finished table's rows line contradicts its bar", surface)
				assert.Contains(t, out, "48.61%", "%s: the copying table keeps its own percent", surface)
				assert.Contains(t, out, "Rows: 3,500,000 / 7,200,000", "%s: the copying table keeps its rows line", surface)
			}
			// The finished table counts its whole estimate in the summary:
			// 200,000 + 3,500,000 of 7,400,000.
			assert.Contains(t, pr, "2 running (50.00%)", "the summary counts the finished table in full")
		})
	}
}

// The users table's copy finished short of its estimate, inserting
// 3,261,100,506 of an estimated 3,291,032,158 rows. The apply then halts: it
// is stopped at the cutover gate, cancelled, or fails at cutover. Both
// surfaces read the halted table as fully copied, with the rows it copied, and
// never as a percent of the estimate or with a rows line against it (UX-8).
func TestHaltedFinishedCopyReadsAsCopied(t *testing.T) {
	for _, tc := range []struct {
		name       string
		applyState string
		taskState  string
		cliWord    string
		prWord     string
	}{
		{name: "stopped at the cutover gate", applyState: state.Apply.Stopped, taskState: state.Task.Stopped},
		{name: "cancelled", applyState: state.Apply.Cancelled, taskState: state.Task.Cancelled, cliWord: "Cancelled at 100.00%", prWord: "Cancelled at 100.00%"},
		{name: "failed at cutover", applyState: state.Apply.Failed, taskState: state.Task.Failed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := &apitypes.ProgressResponse{
				ApplyID: "apply-9c0d1e2f", Database: "shop", Environment: "staging",
				State: tc.applyState, Engine: storage.EngineSpirit,
				Tables: []*apitypes.TableProgressResponse{
					{TableName: "users", DDL: "ALTER TABLE users ADD UNIQUE KEY uk_handle (handle)", ChangeType: "alter",
						Status: tc.taskState, RowsCopied: 3_261_100_506, RowsTotal: 3_291_032_158, PercentComplete: 100},
				},
			}
			data := ParseProgressResponse(resp)
			require.Len(t, data.Tables, 1)
			cli := ansiEscape.ReplaceAllString(FormatTableProgress(data.Tables[0]), "")
			pr := webhooktemplates.RenderApplyStatusComment(webhooktemplates.ApplyStatusFromProgress(resp, "octocat"))

			for surface, out := range map[string]string{"CLI": cli, "PR comment": pr} {
				assert.NotContains(t, out, "99.09%", "%s: the finished table reads as a percent of its estimate", surface)
				assert.NotContains(t, out, "/ 3,291,032,158", "%s: the finished table's rows line contradicts its full bar", surface)
			}
			if tc.cliWord != "" {
				assert.Contains(t, cli, tc.cliWord)
			}
			if tc.prWord != "" {
				assert.Contains(t, pr, tc.prWord)
			}
		})
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
	// planned marks a total that counts the plan's estimate for the shards
	// that have not reported, which then spans the whole table.
	planned bool
	// throttleReason, when set, marks the table as throttled by the engine
	// for that reason.
	throttleReason string
	headline       string
	// rowsLine, when set, is the rows line both surfaces show.
	rowsLine string
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
		{name: "later shards counted from the plan", rowsCopied: 914_707, rowsTotal: 4_000_000, reporting: 1, shards: 4, planned: true,
			headline: "22.87%", rowsLine: "Rows: 914,707 / ~4M · ETA: ≥ 3m 15s"},
		{name: "a shard held back by the throttler", rowsCopied: 914_707, rowsTotal: 1_466_232, reporting: 2, shards: 2, throttleReason: "replica-lag 5s >= 2s",
			headline: "62.38% (throttled)", rowsLine: "Rows: 914,707 / 1,466,233 · ETA: 3m 15s"},
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
			throttled := tc.throttleReason != ""
			cli := FormatTableProgress(TableProgress{
				TableName: "orders", Status: state.Task.Running, ChangeType: "alter",
				RowsCopied: tc.rowsCopied, RowsTotal: total, RowsTotalPlanned: tc.planned, ETASeconds: 195, PlannedShards: tc.shards, Shards: cliShards,
				Throttled: throttled, ThrottleReason: tc.throttleReason,
			})
			pr := webhooktemplates.RenderShardedApplyComment(webhooktemplates.ShardedApplyData{
				State: state.Apply.Running, Environment: "staging", Database: "shop", ApplyID: "apply-5e6f7a8b",
				Keyspaces: []webhooktemplates.ShardedKeyspace{{Keyspace: "shop_sharded", Tables: []webhooktemplates.ShardedTableStatus{{
					Table: "orders", Status: state.Task.Running, RowsCopied: tc.rowsCopied, RowsTotal: total, ETASeconds: 195,
					ShardsReporting: tc.reporting, RowsTotalPlanned: tc.planned, PlannedShards: tc.shards, Shards: prShards,
					Throttled: throttled, ThrottleReason: tc.throttleReason,
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
					RowsCopied: tc.rowsCopied, RowsTotal: total, RowsTotalPlanned: tc.planned, ETASeconds: 195, PlannedShards: int32(tc.shards), Shards: apiShards,
					Throttled: throttled, ThrottleReason: tc.throttleReason,
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
			if tc.rowsLine != "" {
				assert.Equal(t, tc.rowsLine, withoutShardList(cliDetails)[0], "rows line")
			}
		})
	}
}

// A sharded table whose first shard has copied its 1,000 rows and moved on
// to verifying, beside a queued shard the plan estimates at 9,000 rows, reads
// 10% on every surface, the same fraction its rows line shows: the bar that
// stays across shards in different phases counts the queued shard's estimate,
// so it does not jump to half done when the first shard leaves its copy
// (UX-8).
func TestShardedPhaseBarCountsThePlannedRows(t *testing.T) {
	shards := []*apitypes.ShardProgressResponse{
		{Shard: "-80", Status: state.Task.Checksumming, RowsCopied: 1000, RowsTotal: 1000, PercentComplete: 100, ChecksumRowsChecked: 250, ChecksumRowsTotal: 1000},
		{Shard: "80-", Status: state.Task.Pending},
	}
	table := &apitypes.TableProgressResponse{
		TableName: "orders", Keyspace: "shop_sharded", DDL: "ALTER TABLE orders ADD COLUMN note text", ChangeType: "alter",
		Status: state.Task.Checksumming, RowsCopied: 1000, RowsTotal: 10_000, RowsTotalPlanned: true, PlannedShards: 2, Shards: shards,
	}
	cli := FormatTableProgress(ParseProgressResponse(&apitypes.ProgressResponse{State: state.Apply.Running, Sharded: true, Tables: []*apitypes.TableProgressResponse{table}}).Tables[0])
	pr := webhooktemplates.RenderShardedApplyComment(webhooktemplates.ShardedApplyData{
		State: state.Apply.Running, Environment: "staging", Database: "shop", ApplyID: "apply-5e6f7a8b",
		Keyspaces: []webhooktemplates.ShardedKeyspace{{Keyspace: "shop_sharded", Tables: []webhooktemplates.ShardedTableStatus{{
			Table: "orders", Status: state.Task.Checksumming, RowsCopied: 1000, RowsTotal: 10_000, ShardsReporting: 1, RowsTotalPlanned: true, PlannedShards: 2,
			Shards: []webhooktemplates.ShardProgressData{
				{Shard: "-80", Status: state.Task.Checksumming, PercentComplete: 100, RowsCopied: 1000, RowsTotal: 1000, ChecksumRowsChecked: 250, ChecksumRowsTotal: 1000},
				{Shard: "80-", Status: state.Task.Pending},
			},
		}}}},
	})

	status := webhooktemplates.RenderApplyStatusComment(webhooktemplates.ApplyStatusFromProgress(&apitypes.ProgressResponse{
		ApplyID: "apply-5e6f7a8b", Database: "shop", Environment: "staging", State: state.Apply.Running,
		Tables: []*apitypes.TableProgressResponse{table},
	}, "octocat"))

	cliHeadline, cliDetails := cliInFlightReading(t, cli)
	prHeadline, prDetails := shardedPRInFlightReading(t, pr)
	statusHeadline, statusDetails := shardedPRInFlightReading(t, status)
	assert.True(t, hasBar(cliHeadline), "the table keeps its bar across shards in different phases: %s", cliHeadline)
	assert.Equal(t, "10%", withoutBar(cliHeadline), "CLI headline")
	assert.Equal(t, withoutBar(cliHeadline), withoutBar(prHeadline), "the sharded PR comment's headline reads as the CLI's")
	assert.Equal(t, withoutBar(cliHeadline), withoutBar(statusHeadline), "the apply status comment's headline reads as the CLI's")
	require.NotEmpty(t, withoutShardList(cliDetails), "the table has a rows line")
	assert.Equal(t, "Rows: 1,000 / ~10k", withoutShardList(cliDetails)[0], "rows line")
	assert.Equal(t, withoutShardList(cliDetails), withoutShardList(prDetails), "the sharded PR comment's detail lines read as the CLI's")
	assert.Equal(t, withoutShardList(cliDetails), withoutShardList(statusDetails), "the apply status comment's detail lines read as the CLI's")
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

// hasBar reports whether a headline draws a progress bar.
func hasBar(headline string) bool {
	return barGlyphs.MatchString(headline)
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
