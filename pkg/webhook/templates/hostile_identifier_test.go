package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/ui"
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
	hostileItem := SchemaChangeReconciliationItem{Database: hostileIdentifier, Environment: hostileIdentifier, ApplyID: hostileIdentifier, State: hostileIdentifier}
	reconcileOne := func(inProgress bool) string {
		item := hostileItem
		item.InProgress = inProgress
		return RenderSchemaChangeReconciliationRequired(SchemaChangeReconciliationData{Items: []SchemaChangeReconciliationItem{item}})
	}

	surfaces := map[string]string{
		"plan comment":                                 RenderPlanComment(plan),
		"unsafe changes rejection":                     RenderUnsafeChangesBlocked(plan),
		"blocked changes rejection":                    RenderBlockedChangesApplyRejected(plan),
		"apply retry row":                              RenderApplyStatusComment(apply),
		"sharded apply shard table":                    shardTable.String(),
		"config not authorized comment":                RenderConfigNotAuthorized(configNotAuthorized),
		"config not authorized error line":             RenderConfigNotAuthorizedLine(hostileIdentifier, hostileIdentifier),
		"unmanaged schema config notice":               RenderUnmanagedSchemaConfigsNotice([]string{hostileIdentifier}, []UnmanagedSchemaConfigNoticeData{{Database: hostileIdentifier, SchemaPath: hostileIdentifier}}),
		"unmanaged schema passing check summary":       unmanagedSchemaPassingCheckSummary(hostileIdentifier),
		"invalid environment available list":           RenderInvalidEnv("apply", []string{hostileIdentifier}),
		"database not found for a hostile -d":          RenderDatabaseNotFound(SchemaErrorData{DatabaseName: hostileIdentifier}),
		"scoped database not found for a hostile -d":   RenderDatabaseNotFound(SchemaErrorData{DatabaseName: hostileIdentifier, SearchedDirs: []string{"schema"}}),
		"database not configured for a hostile -d":     RenderDatabaseNotConfigured(SchemaErrorData{DatabaseName: hostileIdentifier}),
		"database not registered for a hostile config": RenderDatabaseNotRegistered(SchemaErrorData{UnregisteredConfigs: []UnregisteredSchemaConfigData{{Database: hostileIdentifier, SchemaPath: hostileIdentifier}}}),
		"databases not registered for hostile configs": RenderDatabaseNotRegistered(SchemaErrorData{UnregisteredConfigs: []UnregisteredSchemaConfigData{{Database: hostileIdentifier, SchemaPath: hostileIdentifier}, {Database: "orders", SchemaPath: hostileIdentifier}}}),
		"repository truncated for a hostile -d":        RenderRepositoryTreeTruncated(SchemaErrorData{DatabaseName: hostileIdentifier}),
		"database rejects the repo for a hostile -d":   RenderDatabaseRepoNotAllowed(SchemaErrorData{DatabaseName: hostileIdentifier}),
		"no config for a hostile -d":                   RenderNoConfig(SchemaErrorData{DatabaseName: hostileIdentifier}),

		"no managed schema changes for a hostile -e":      RenderNoManagedSchemaChanges(SchemaErrorData{Environment: hostileIdentifier}),
		"command for a database that is not configured":   RenderPRCommandDatabaseNotConfigured(ActorAuthorizationCommentData{CommandName: "apply", Database: hostileIdentifier, Environment: hostileIdentifier}),
		"reconciliation of one completed apply":           reconcileOne(false),
		"reconciliation of one in-progress apply":         reconcileOne(true),
		"reconciliation table of several applies":         RenderSchemaChangeReconciliationRequired(SchemaChangeReconciliationData{Items: []SchemaChangeReconciliationItem{hostileItem, hostileItem}}),
		"reconciliation table row without an environment": RenderSchemaChangeReconciliationRequired(SchemaChangeReconciliationData{Items: []SchemaChangeReconciliationItem{{Database: hostileIdentifier}, {Database: hostileIdentifier}}}),
		"reconciliation of several in-progress applies":   RenderSchemaChangeReconciliationRequired(SchemaChangeReconciliationData{Items: []SchemaChangeReconciliationItem{{Database: hostileIdentifier, InProgress: true}, hostileItem}}),
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

// A pipe in a database, environment or apply ID the PR author chose stays in
// its table cell, so the row keeps its four columns.
func TestReconciliationTableKeepsAPipeInsideItsCell(t *testing.T) {
	rendered := RenderSchemaChangeReconciliationRequired(SchemaChangeReconciliationData{Items: []SchemaChangeReconciliationItem{
		{Database: "orders|x", Environment: "staging", ApplyID: "apply-1", State: "running"},
		{Database: "users", Environment: "stag|ing", ApplyID: "ap|ply-2", State: "completed"},
	}})

	assert.Contains(t, rendered, "| `orders\\|x` | `staging` | `apply-1` | `running` |\n")
	assert.Contains(t, rendered, "| `users` | `stag\\|ing` | `ap\\|ply-2` | `completed` |\n")
}

// A lint message or unsafe change reason is written by a linter or an engine
// from names in the PR author's schema, so it can carry anything a schema name
// can. Wherever it lands in the comment it reads as the text it is: no link,
// image, HTML tag, emphasis, heading, fence or mention that the author could
// use to speak in the comment, and no line break to start one on.
func TestHostileReasonStaysPlainTextInPlanComment(t *testing.T) {
	tests := []struct {
		name    string
		reason  string
		written string
	}{
		{"link", `see [the docs](https://evil.example/x) first`, `see \[the docs\](https://evil.example/x) first`},
		{"image", `![x](https://evil.example/p.png)`, `!\[x\](https://evil.example/p.png)`},
		{"HTML tag", `<img src=x onerror=alert(1)>`, `\<img src=x onerror=alert(1)>`},
		{"emphasis", `*all clear* and __trusted__`, `\*all clear\* and \_\_trusted\_\_`},
		{"user mention", `ask @octocat to approve`, "ask `@octocat` to approve"},
		{"team mention", `ask @acme/db-admins to approve`, "ask `@acme/db-admins` to approve"},
		{"table separator", `a | b`, `a \| b`},
		{"line breaks", "x\n# injected\n```\nsql\n```", "x # injected ``` sql ```"},
		{"unbalanced backtick", "ends a span ` early", "ends a span \\` early"},
		{"backslash before a backtick", "\\` @octocat `", "\\\\` @octocat `"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := PlanCommentData{
				Database: "testapp", Environment: "staging", IsMySQL: true,
				Changes:          []KeyspaceChangeData{{Keyspace: "testapp", Statements: []string{"ALTER TABLE t ADD c int"}}},
				HasUnsafeChanges: true,
				UnsafeChanges:    []UnsafeChangeData{{Table: "orders", Reason: tt.reason, ChangeType: "DROP COLUMN"}},
				LintViolations:   []LintViolationData{{Table: "orders", Message: tt.reason}},
			}

			for name, rendered := range map[string]string{
				"plan comment":             RenderPlanComment(plan),
				"unsafe changes rejection": RenderUnsafeChangesBlocked(plan),
			} {
				assert.Contains(t, rendered, "1. `orders`: "+tt.written+"\n", name)
			}
			assert.Contains(t, RenderPlanComment(plan), "- `orders`: "+tt.written+"\n", "the lint line")
		})
	}
}

// A reason that quotes identifiers renders them as code exactly as
// ui.CodeQuoteIdentifiers does, and a reason with nothing to escape comes out
// as that function leaves it, so existing comments do not change.
func TestMarkdownReasonKeepsWhatCodeQuoteIdentifiersWrites(t *testing.T) {
	for _, reason := range []string{
		`Index "idx_status" should be made invisible before dropping`,
		`Column "created_at" of type "DATETIME" should not be the leftmost index column`,
		`Using "mediumint(9)" as primary key is discouraged`,
		`Column "status" in table "orders" has type "enum('active','archived')"`,
		`Table "app.users" has no primary key`,
		`The statement "DROP COLUMN ` + "`legacy`" + `" cannot be undone`,
		"has_timestamp: column seen_at uses TIMESTAMP",
		"range predicates (>, >=, <, <=, BETWEEN) stop the optimizer using later columns",
		"drops column nickname (data loss) and index idx_nick",
	} {
		t.Run(reason, func(t *testing.T) {
			assert.Equal(t, ui.CodeQuoteIdentifiers(reason), markdownReason(reason))
		})
	}

	assert.Equal(t, "Index `idx_status` should be made invisible before dropping",
		markdownReason(`Index "idx_status" should be made invisible before dropping`))
	assert.Equal(t, `The index `+"`idx_a`"+` is marked "do not drop"`,
		markdownReason(`The index "idx_a" is marked "do not drop"`),
		"quoted prose is not an identifier and keeps its quotes")
}

// Text a reason has inside a code span is left as written, and what surrounds
// the span is escaped, so a hostile name that code quoting recognises cannot be
// escaped twice and a hostile one it does not recognise cannot slip out.
func TestMarkdownReasonEscapesOnlyOutsideCodeSpans(t *testing.T) {
	tests := []struct {
		name   string
		reason string
		want   string
	}{
		{"quoted name beside link", `Column "a_b" see [x](y)`, "Column `a_b` see \\[x\\](y)"},
		{"mention in a span is not wrapped again", "ask `@octocat` and @hubot", "ask `@octocat` and `@hubot`"},
		{"span with a longer delimiter", "see ``a ` b`` then <b>", "see ``a ` b`` then \\<b>"},
		{"intraword underscore stays", "col_a and _lead and trail_ and a__b", "col_a and \\_lead and trail\\_ and a\\_\\_b"},
		{"less than sign that starts no HTML", "a < b, a <= b, 1<2, <", "a < b, a <= b, 1<2, <"},
		{"less than sign that starts HTML", "<b>x</b> <!-- y --> <?z", "\\<b>x\\</b> \\<!-- y --> \\<?z"},
		{"email address is not a mention", "mail dba@example.com", "mail dba@example.com"},
		{"mention after punctuation", "(@octocat)", "(`@octocat`)"},
		{"mentions that touch share one span", "@octocat@hubot and @acme/db-admins@hubot", "`@octocat@hubot` and `@acme/db-admins@hubot`"},
		{"character references cannot spell an @", "&#64;octocat &commat;octocat &#x40;octocat", "\\&#64;octocat \\&commat;octocat \\&#x40;octocat"},
		{"mention before a span", "@octocat``a ` b`` @hubot`c`", "`@octocat` ``a ` b`` `@hubot` `c`"},
		{"mention after a span", "``a ` b``@octocat `c`@hubot", "``a ` b`` `@octocat` `c` `@hubot`"},
		{"strikethrough", "~~all clear~~", "\\~\\~all clear\\~\\~"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, markdownReason(tt.reason))
		})
	}
}

// A mention never reaches the comment as prose, whatever sits beside it: not
// next to another mention, not next to a code span the reason already has, and
// not spelled as a character reference.
func TestMarkdownReasonDefusesEveryMention(t *testing.T) {
	for _, reason := range []string{
		"@octocat@hubot",
		"@acme/db-admins@hubot",
		"ask &#64;octocat to approve",
		"ask &commat;octocat to approve",
		"ask &#x40;octocat to approve",
		"@octocat``a ` b``",
		"``a ` b``@octocat",
		"`a`@octocat`b`@hubot",
	} {
		t.Run(reason, func(t *testing.T) {
			mapOutsideCodeSpans(markdownReason(reason), func(prose string) string {
				assert.NotContains(t, prose, "@", "an @ outside a code span can notify")
				assert.NotContains(t, strings.ReplaceAll(prose, `\&`, ""), "&", "an ampersand outside a code span can start a character reference that spells an @")
				return prose
			})
		})
	}
}

func unmanagedSchemaPassingCheckSummary(identifier string) string {
	_, summary := RenderUnmanagedSchemaPassingCheck(identifier, []UnmanagedSchemaConfigNoticeData{{Database: identifier, SchemaPath: identifier}})
	return summary
}
