package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
)

// hostileIdentifier is a name a PR author controls — a table, shard, database,
// or schema directory — built to break out of an inline code span: it closes a
// span with its own backtick, then opens a code fence and a heading on lines of
// their own.
const hostileIdentifier = "x`\n```\n# injected\n"

// Every surface that renders an identifier the PR author chose keeps it inside
// one code span: the identifier is flattened onto the line that names it, so
// the fence and heading it carries never reach the comment as lines of their
// own.
func TestHostileIdentifierNeverEscapesItsCodeSpan(t *testing.T) {
	sharded := []string{hostileIdentifier}
	plan := PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes:          []KeyspaceChangeData{{Keyspace: "testapp", Statements: []string{"ALTER TABLE t ADD c int"}}},
		HasUnsafeChanges: true,
		UnsafeChanges:    []UnsafeChangeData{{Table: hostileIdentifier, Reason: "drops a column", ChangeType: "DROP COLUMN"}},
		BlockedChanges:   []BlockedChangeData{{Table: hostileIdentifier, Reason: "engine refuses", Shards: sharded, TotalShards: 2}},
		DirectChanges:    []DirectChangeData{{Table: hostileIdentifier, Reason: "runs directly", Shards: sharded, TotalShards: 2}},
		AttributedChanges: []AttributedChangeData{
			{Table: hostileIdentifier, Repository: "org/repo", PullRequest: 7},
			{Table: hostileIdentifier, Unresolved: true},
		},
		ExemptTables: []ExemptTablesData{{Namespace: hostileIdentifier, Tables: []string{hostileIdentifier}, Reason: "archive naming"}},
	}
	apply := ApplyStatusCommentData{
		Database: "testapp", Environment: "staging",
		State: state.Apply.FailedRetryable, Attempt: 1,
		Tables: []TableProgressData{{TableName: hostileIdentifier, Status: state.Task.FailedRetryable, DDL: "ALTER TABLE t ADD c int"}},
	}
	configNotAuthorized := SchemaErrorData{DatabaseName: hostileIdentifier, SchemaPath: hostileIdentifier}
	var shardTable strings.Builder
	writeShardStatusTable(&shardTable, []ShardStatus{{Shard: hostileIdentifier, State: state.Apply.Running}})

	surfaces := map[string]string{
		"plan comment":                        RenderPlanComment(plan),
		"unsafe changes rejection":            RenderUnsafeChangesBlocked(plan),
		"blocked changes rejection":           RenderBlockedChangesApplyRejected(plan),
		"apply retry row":                     RenderApplyStatusComment(apply),
		"sharded apply shard table":           shardTable.String(),
		"config not authorized comment":       RenderConfigNotAuthorized(configNotAuthorized),
		"config not authorized error line":    RenderConfigNotAuthorizedLine(hostileIdentifier, hostileIdentifier),
		"unmanaged schema config notice":      RenderUnmanagedSchemaConfigsNotice([]UnmanagedSchemaConfigNoticeData{{Database: hostileIdentifier, SchemaPath: hostileIdentifier}}),
		"invalid environment available list":  RenderInvalidEnv("apply", []string{hostileIdentifier}),
		"database not found for a hostile -d": RenderDatabaseNotFound(SchemaErrorData{DatabaseName: hostileIdentifier}),
		"no config for a hostile -d":          RenderNoConfig(SchemaErrorData{DatabaseName: hostileIdentifier}),
	}

	for name, rendered := range surfaces {
		t.Run(name, func(t *testing.T) {
			require.Contains(t, rendered, "injected", "the surface renders the identifier at all")
			assert.NotContains(t, rendered, hostileIdentifier, "the identifier never reaches the comment as written")
			for line := range strings.SplitSeq(rendered, "\n") {
				if !strings.Contains(line, "injected") {
					continue
				}
				assert.False(t, strings.HasPrefix(line, "#"), "the identifier's heading never opens a line: %q", line)
				assert.False(t, strings.HasPrefix(line, "```"), "the identifier's fence never opens a line: %q", line)
				assert.Contains(t, line, "x` ``` # injected", "the identifier is flattened onto the line that names it: %q", line)
			}
		})
	}
}
