package commands

import (
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/cmd/templates"
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
