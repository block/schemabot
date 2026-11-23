package templates

import (
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/ui"
	webhooktemplates "github.com/block/schemabot/pkg/webhook/templates"
)

// postgresProgress is a PostgreSQL apply running one statement, as the
// progress API reports it. metadata is the engine's build work, if any.
func postgresProgress(ddl string, percent int32, metadata map[string]string) *apitypes.ProgressResponse {
	md := map[string]string{"step": "1", "steps_total": "1", "statement": ddl}
	maps.Copy(md, metadata)
	return &apitypes.ProgressResponse{
		ApplyID: "apply-a1b2c3d4e5f6", Database: "shop", Environment: "staging",
		State: state.Apply.Running, Engine: storage.EnginePostgres, DatabaseType: storage.DatabaseTypePostgres,
		Metadata: md,
		Tables: []*apitypes.TableProgressResponse{{
			TableName: "users", DDL: ddl, ChangeType: "create", Status: state.Task.Running, PercentComplete: percent,
		}},
	}
}

// An operator following a PostgreSQL index build from the terminal and from
// the PR sees one bar with one percent: both surfaces draw the engine's
// whole-build percent from the same table row, and the phase line under the
// step carries counters only, so neither surface shows a second percent.
func TestIndexBuildProgressReadsTheSameOnEverySurface(t *testing.T) {
	resp := postgresProgress("CREATE INDEX CONCURRENTLY idx_users_email ON users (email)", 25, map[string]string{
		"executor_operation": "concurrent-index-build", "server_phase": "building index: scanning table",
		"blocks_done": "30000", "blocks_total": "48000",
	})
	want := ui.IndexBuildProgress(25)

	data := ParseProgressResponse(resp)
	require.Len(t, data.Tables, 1)
	cli := FormatTableProgress(data.Tables[0])
	pr := webhooktemplates.RenderApplyStatusComment(webhooktemplates.ApplyStatusFromProgress(resp, "octocat"))

	assert.Contains(t, cli, "users: "+want+"\n")
	assert.Contains(t, pr, "**`users`**: "+want+"\n")
	assert.Contains(t, pr, "building index: 30,000 of 48,000 blocks\n")
	assert.NotContains(t, pr, "62%", "the phase line must not show a phase-local percent beside the bar")
}

// A PostgreSQL statement that is not an index build has no percent to show,
// so neither surface draws a bar for it.
func TestPostgresPlainDDLDrawsNoBar(t *testing.T) {
	resp := postgresProgress("ALTER TABLE users ADD COLUMN nickname text", 0, nil)

	data := ParseProgressResponse(resp)
	require.Len(t, data.Tables, 1)
	assert.False(t, data.Tables[0].IndexBuild)
	assert.NotContains(t, FormatTableProgress(data.Tables[0]), "index build")

	pr := webhooktemplates.RenderApplyStatusComment(webhooktemplates.ApplyStatusFromProgress(resp, "octocat"))
	assert.Contains(t, pr, "**`users`**: Running...\n")
}
