package commands

import (
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/cmd/templates"
	"github.com/block/schemabot/pkg/state"
	"github.com/charmbracelet/bubbles/spinner"
	"github.com/stretchr/testify/assert"
)

// TestTUIShardRendering verifies that the TUI's shard progress conversion
// and shared template rendering produce expected output.
func TestTUIShardRendering(t *testing.T) {
	tests := []struct {
		name     string
		tables   []tableProgress
		contains []string
		absent   []string
	}{
		{
			name: "keyspace header shown for vitess tables",
			tables: []tableProgress{
				{Name: "users", Keyspace: "myapp_sharded", DDL: "ALTER TABLE users ADD COLUMN x int", Status: "pending"},
			},
			contains: []string{"myapp_sharded"},
		},
		{
			name: "no keyspace header for mysql tables",
			tables: []tableProgress{
				{Name: "users", DDL: "ALTER TABLE users ADD COLUMN x int", Status: "pending"},
			},
			absent: []string{"──"},
		},
		{
			name: "multiple keyspaces grouped",
			tables: []tableProgress{
				{Name: "users", Keyspace: "app", Status: "completed"},
				{Name: "orders", Keyspace: "app_sharded", Status: "pending"},
			},
			contains: []string{"app", "app_sharded"},
		},
		{
			name: "shard progress rendered via shared templates",
			tables: []tableProgress{
				{
					Name: "users", Keyspace: "myapp", Status: "running",
					Shards: []shardProgress{
						{Shard: "-80", Status: "running", RowsCopied: 500, RowsTotal: 1000},
						{Shard: "80-", Status: "running", RowsCopied: 300, RowsTotal: 1000},
					},
				},
			},
			contains: []string{"Shards:", "2 copying"},
		},
		{
			name: "uppercase and prefixed statuses normalized for rendering",
			tables: []tableProgress{
				{
					Name: "users", Keyspace: "myapp", Status: "STATE_RUNNING",
					RowsCopied: 500, RowsTotal: 1000,
					Shards: []shardProgress{
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
			tplTables := toTemplateTables(tt.tables)

			var b strings.Builder
			hasNS := false
			for _, tbl := range tplTables {
				if tbl.Namespace != "" {
					hasNS = true
					break
				}
			}
			if hasNS {
				b.WriteString(templates.FormatNamespacedTables(tplTables))
			} else {
				for _, tbl := range tplTables {
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
		tables   []tableProgress
		contains []string
		absent   []string
	}{
		{
			name:  "applying branch changes shows status_detail counter",
			state: state.Apply.ApplyingBranchChanges,
			metadata: map[string]string{
				"status_detail": "Applied keyspace myapp_sharded_003 (8/12)",
			},
			tables: []tableProgress{
				{Name: "users", Keyspace: "myapp_sharded", Status: "pending"},
			},
			contains: []string{"Applied keyspace myapp_sharded_003 (8/12)"},
			absent:   []string{"Queued", "users", "──"},
		},
		{
			name:  "applying branch changes without status_detail shows default",
			state: state.Apply.ApplyingBranchChanges,
			tables: []tableProgress{
				{Name: "users", Keyspace: "myapp_sharded", Status: "pending"},
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
