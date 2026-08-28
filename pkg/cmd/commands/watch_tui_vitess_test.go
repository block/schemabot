package commands

import (
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/state"
	"github.com/charmbracelet/bubbles/spinner"
	"github.com/stretchr/testify/assert"
)

// TestTUIShardRendering verifies that the watch TUI's progress parsing and
// shared template rendering produce expected output from an API response,
// including keyspace grouping, shard rollups, and raw engine status
// normalization.
func TestTUIShardRendering(t *testing.T) {
	tests := []struct {
		name     string
		tables   []*apitypes.TableProgressResponse
		contains []string
		absent   []string
	}{
		{
			name: "keyspace header shown for vitess tables",
			tables: []*apitypes.TableProgressResponse{
				{TableName: "users", Keyspace: "myapp_sharded", DDL: "ALTER TABLE users ADD COLUMN x int", Status: "pending"},
			},
			contains: []string{"myapp_sharded"},
		},
		{
			name: "no keyspace header for mysql tables",
			tables: []*apitypes.TableProgressResponse{
				{TableName: "users", DDL: "ALTER TABLE users ADD COLUMN x int", Status: "pending"},
			},
			absent: []string{"──"},
		},
		{
			name: "multiple keyspaces grouped",
			tables: []*apitypes.TableProgressResponse{
				{TableName: "users", Keyspace: "app", Status: "completed"},
				{TableName: "orders", Keyspace: "app_sharded", Status: "pending"},
			},
			contains: []string{"── app ──", "── app_sharded ──"},
		},
		{
			name: "shard progress rendered via shared templates",
			tables: []*apitypes.TableProgressResponse{
				{
					TableName: "users", Keyspace: "myapp", Status: "running",
					Shards: []*apitypes.ShardProgressResponse{
						{Shard: "-80", Status: "running", RowsCopied: 500, RowsTotal: 1000, ETASeconds: 150},
						{Shard: "80-", Status: "running", RowsCopied: 300, RowsTotal: 1000},
					},
				},
			},
			// The ETA pins the shard-field plumbing end to end: unlike the
			// percent, formatShardLine cannot re-derive it from row counts.
			contains: []string{"Shards:", "2 copying", "ETA 2m 30s"},
		},
		{
			name: "uppercase and prefixed statuses normalized for rendering",
			tables: []*apitypes.TableProgressResponse{
				{
					TableName: "users", Keyspace: "myapp", Status: "STATE_RUNNING",
					RowsCopied: 500, RowsTotal: 1000,
					Shards: []*apitypes.ShardProgressResponse{
						{Shard: "-80", Status: "STATE_RUNNING", RowsCopied: 300, RowsTotal: 500},
						{Shard: "80-", Status: "RUNNING", RowsCopied: 200, RowsTotal: 500},
					},
				},
			},
			contains: []string{"Rows:", "1,000", "Shards:", "2 copying"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := parseProgressResult(&apitypes.ProgressResponse{Tables: tt.tables})

			var b strings.Builder
			hasNS := false
			for _, tbl := range msg.tables {
				if tbl.Namespace != "" {
					hasNS = true
					break
				}
			}
			if hasNS {
				b.WriteString(templates.FormatNamespacedTables(msg.tables))
			} else {
				for _, tbl := range msg.tables {
					b.WriteString(templates.FormatTableProgress(tbl))
				}
			}

			result := b.String()
			for _, expected := range tt.contains {
				assert.Contains(t, result, expected)
			}
			for _, unexpected := range tt.absent {
				assert.NotContains(t, result, unexpected)
			}
		})
	}
}

func TestTUIBranchApplyProgress(t *testing.T) {
	tests := []struct {
		name     string
		state    string
		metadata map[string]string
		tables   []templates.TableProgress
		contains []string
		absent   []string
	}{
		{
			name:  "applying branch changes shows status_detail counter",
			state: state.Apply.ApplyingBranchChanges,
			metadata: map[string]string{
				"status_detail": "Applied keyspace myapp_sharded_003 (8/12)",
			},
			tables: []templates.TableProgress{
				{TableName: "users", Namespace: "myapp_sharded", Status: "pending"},
			},
			contains: []string{"Applied keyspace myapp_sharded_003 (8/12)"},
			absent:   []string{"Queued", "users", "──"},
		},
		{
			name:  "applying branch changes without status_detail shows default",
			state: state.Apply.ApplyingBranchChanges,
			tables: []templates.TableProgress{
				{TableName: "users", Namespace: "myapp_sharded", Status: "pending"},
			},
			contains: []string{"Applying changes to branch..."},
			absent:   []string{"Queued", "users", "──"},
		},
		{
			name:  "preparing branch with existing_branch shows refreshing",
			state: state.Apply.PreparingBranch,
			metadata: map[string]string{
				"existing_branch": "my-branch",
			},
			contains: []string{"Refreshing branch schema..."},
		},
		{
			name:  "preparing branch with status_detail overrides label",
			state: state.Apply.PreparingBranch,
			metadata: map[string]string{
				"existing_branch": "my-branch",
				"status_detail":   "Refreshing schema for branch my-branch from main",
			},
			contains: []string{"Refreshing schema for branch my-branch from main"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := WatchModel{
				state:       tt.state,
				metadata:    tt.metadata,
				tables:      tt.tables,
				initialized: true,
				spinner:     spinner.New(),
				engine:      "PlanetScale",
			}

			view := m.progressView()
			for _, expected := range tt.contains {
				assert.Contains(t, view, expected, "expected %q in TUI output", expected)
			}
			for _, unexpected := range tt.absent {
				assert.NotContains(t, view, unexpected, "unexpected %q in TUI output", unexpected)
			}
		})
	}
}

// The deploy request link stays visible while the apply is running, not only
// during deploy-request setup, so an operator watching a long copy can open the
// PlanetScale console to inspect it.
func TestTUIShowsDeployRequestURLWhileRunning(t *testing.T) {
	const url = "https://app.planetscale.com/block-staging/boardgames/deploy-requests/106"
	m := WatchModel{
		state:       state.Apply.Running,
		metadata:    map[string]string{"deploy_request_url": url},
		tables:      []templates.TableProgress{{TableName: "customers", Namespace: "boardgames_sharded", Status: "running"}},
		initialized: true,
		spinner:     spinner.New(),
		engine:      "PlanetScale",
	}

	view := m.progressView()
	assert.Contains(t, view, "Deploy Request:  "+url)
}
