package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
)

// An error message quotes the PR author's input, so an @handle in it must not
// notify anyone. Mentions outside a code span are written as inline code; code
// spans SchemaBot wrote stay as they are; HTML escaping stays as it was.
func TestErrorDetailMarkdown(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "a user mention and a team mention are written as code",
			in:   "ask @octocat or @acme/dba-team",
			want: "ask <code>@octocat</code> or <code>@acme/dba-team</code>",
		},
		{
			name: "a mention inside quotes keeps the escaped quotes",
			in:   `database "@acme/dba-team" is not configured on this server`,
			want: "database &#34;<code>@acme/dba-team</code>&#34; is not configured on this server",
		},
		{
			name: "a span SchemaBot wrote is unchanged",
			in:   "SchemaBot could not reach the remote deployment `pie` for target `orders-staging`.",
			want: "SchemaBot could not reach the remote deployment `pie` for target `orders-staging`.",
		},
		{
			name: "a mention already inside a code span is unchanged",
			in:   "database `@acme/dba-team` is not configured",
			want: "database `@acme/dba-team` is not configured",
		},
		{
			name: "only the mention outside the span is rewritten",
			in:   "`@octocat` and @hubot",
			want: "`@octocat` and <code>@hubot</code>",
		},
		{
			name: "HTML is still escaped",
			in:   `<b>bold</b> & 'single' "double"`,
			want: "&lt;b&gt;bold&lt;/b&gt; &amp; &#39;single&#39; &#34;double&#34;",
		},
		{
			name: "a character reference stays text",
			in:   "&#64;octocat and &#x40;octocat",
			want: "&amp;#64;octocat and &amp;#x40;octocat",
		},
		{
			name: "an address names nobody",
			in:   "write to dba@example.com",
			want: "write to dba@example.com",
		},
		{
			name: "mentions that touch share one element",
			in:   "@octocat@hubot",
			want: "<code>@octocat@hubot</code>",
		},
		{
			name: "a lone backtick before a mention cannot pair with anything",
			in:   "unmatched ` before @octocat",
			want: "unmatched ` before <code>@octocat</code>",
		},
		{
			name: "a backslash before a mention cannot hide the element",
			in:   `\@octocat`,
			want: `\\<code>@octocat</code>`,
		},
		{
			name: "an escaped backslash before a mention is left alone",
			in:   `\\@octocat`,
			want: `\\<code>@octocat</code>`,
		},
		{
			name: "text with no mention is only escaped",
			in:   "failed to fetch repository contents",
			want: "failed to fetch repository contents",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, errorDetailMarkdown(tc.in))
		})
	}
}

// The generic command-failure comment holds a multi-line detail inside its
// blockquote and writes the mentions in it as code.
func TestRenderGenericErrorWritesMentionsAsCode(t *testing.T) {
	body := RenderGenericError(SchemaErrorData{
		Timestamp:   "2026-07-16 18:56:00",
		CommandName: "plan",
		ErrorDetail: "database \"@acme/dba-team\" is not configured on this server\nnotify @octocat\nSchemaBot could not reach `pie`.",
	})

	assert.Contains(t, body, "> database &#34;<code>@acme/dba-team</code>&#34; is not configured on this server\n"+
		"> notify <code>@octocat</code>\n"+
		"> SchemaBot could not reach `pie`.")
	assert.NotContains(t, body, " @octocat")
	assert.NotContains(t, body, "\"@acme")
}

// Every renderer that puts an error message into comment markdown writes the
// mentions in it as code.
func TestErrorRenderersWriteMentionsAsCode(t *testing.T) {
	const detail = `database "@acme/dba-team" is not configured, ask @octocat`
	const want = "database &#34;<code>@acme/dba-team</code>&#34; is not configured, ask <code>@octocat</code>"

	failedShard := ShardStatus{Shard: "-80", State: state.ApplyOperation.Failed, Error: detail}
	renderers := map[string]func() string{
		"generic error": func() string {
			return RenderGenericError(SchemaErrorData{Timestamp: "t", CommandName: "apply", ErrorDetail: detail})
		},
		"apply error block": func() string {
			var sb strings.Builder
			writeErrorBlock(&sb, glyph.Failed, detail)
			return sb.String()
		},
		"table error line": func() string {
			var sb strings.Builder
			writeTableErrorLine(&sb, glyph.Failed, detail)
			return sb.String()
		},
		"control rejection": func() string {
			return RenderControlRejections([]ControlRejectionData{{Operation: "stop", Message: detail}})
		},
		"rollback not accepted": func() string {
			return RenderRollbackNotAccepted("orders", "staging", detail)
		},
		"plan errors": func() string {
			var sb strings.Builder
			writeErrors(&sb, []string{detail})
			return sb.String()
		},
		"aggregate first failure": func() string {
			var sb strings.Builder
			writeAggregateFirstFailure(&sb, &presentation.Deployment{Name: "pie", Error: detail})
			return sb.String()
		},
		"first failed shard": func() string {
			var sb strings.Builder
			writeShardedFailure(&sb, ShardedApplyData{Keyspaces: []ShardedKeyspace{{Keyspace: "ks", Shards: []ShardStatus{failedShard}}}})
			return sb.String()
		},
		"apply-level failure": func() string {
			var sb strings.Builder
			writeShardedFailure(&sb, ShardedApplyData{State: state.Apply.Failed, ErrorMessage: detail})
			return sb.String()
		},
		"one reason for several shards": func() string {
			var sb strings.Builder
			second := failedShard
			second.Shard = "80-"
			writeShardFailureReasons(&sb, []ShardStatus{failedShard, second}, 2)
			return sb.String()
		},
		"several reasons for several shards": func() string {
			var sb strings.Builder
			second := ShardStatus{Shard: "80-", State: state.ApplyOperation.Failed, Error: "other " + detail}
			writeShardFailureReasons(&sb, []ShardStatus{failedShard, second}, 2)
			return sb.String()
		},
		"finalizer failure": func() string {
			var sb strings.Builder
			writeFinalizerFailure(&sb, &FinalizerFailure{Keyspace: "ks", State: state.ApplyOperation.Failed, Error: detail})
			return sb.String()
		},
		"shard status cell": func() string {
			return shardStatusCell(failedShard)
		},
	}
	for name, render := range renderers {
		t.Run(name, func(t *testing.T) {
			out := render()
			assert.Contains(t, out, want)
			assert.NotContains(t, strings.ReplaceAll(out, "<code>@", ""), "@octocat", "the only @octocat is inside a code element")
		})
	}
}
