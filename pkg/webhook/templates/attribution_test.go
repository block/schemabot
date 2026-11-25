package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/schema"
)

// ownerDid parses the statements an owning pull request applied into the
// changes an attribution carries.
func ownerDid(t *testing.T, parser ddl.StatementParser, stmts ...string) *ddl.ObjectChanges {
	t.Helper()
	var merged ddl.ObjectChanges
	for _, stmt := range stmts {
		c, err := parser.ObjectChanges(stmt)
		require.NoError(t, err)
		merged.Added = append(merged.Added, c.Added...)
		merged.Dropped = append(merged.Dropped, c.Dropped...)
		merged.Modified = append(merged.Modified, c.Modified...)
		merged.Referenced = append(merged.Referenced, c.Referenced...)
	}
	return &merged
}

// A planned change carries the attribution only when it undoes something the
// owning pull request did to the table. A change to objects the owner never
// touched is this pull request's own, and anything the match cannot read is
// attributed.
func TestUndoesAttributedWork(t *testing.T) {
	mysql, err := ddl.ParserForDialect(schema.DialectMySQL)
	require.NoError(t, err)
	postgres, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)

	tests := []struct {
		name    string
		parser  ddl.StatementParser
		owner   []string
		planned string
		want    bool
	}{
		{
			name:    "renaming a column the owner never touched beside an index it added",
			parser:  mysql,
			owner:   []string{"ALTER TABLE `events` ADD INDEX `idx_created_at` (`created_at`)"},
			planned: "ALTER TABLE `events` DROP COLUMN `payload`, ADD COLUMN `body` text",
			want:    false,
		},
		{
			name:    "dropping the index the owner added",
			parser:  mysql,
			owner:   []string{"ALTER TABLE `events` ADD INDEX `idx_created_at` (`created_at`)"},
			planned: "ALTER TABLE `events` DROP INDEX `idx_created_at`",
			want:    true,
		},
		{
			name:    "dropping an index the owner added without naming it",
			parser:  mysql,
			owner:   []string{"ALTER TABLE `events` ADD INDEX (`created_at`)"},
			planned: "ALTER TABLE `events` DROP INDEX `created_at`",
			want:    true,
		},
		{
			name:    "dropping a key the owner declared on the column it added",
			parser:  mysql,
			owner:   []string{"ALTER TABLE `events` ADD COLUMN `code` int UNIQUE"},
			planned: "ALTER TABLE `events` DROP INDEX `code`",
			want:    true,
		},
		{
			name:    "postgres: dropping an index the owner created without naming it",
			parser:  postgres,
			owner:   []string{"CREATE INDEX ON events (created_at)"},
			planned: "DROP INDEX events_created_at_idx",
			want:    true,
		},
		{
			name:    "dropping the column the owner indexed",
			parser:  mysql,
			owner:   []string{"CREATE INDEX `idx_created_at` ON `events` (`created_at`)"},
			planned: "ALTER TABLE `events` DROP COLUMN `created_at`",
			want:    true,
		},
		{
			name:    "dropping the column the owner added, named in another case",
			parser:  mysql,
			owner:   []string{"ALTER TABLE `users` ADD COLUMN `Email` varchar(255)"},
			planned: "ALTER TABLE `users` DROP COLUMN `email`",
			want:    true,
		},
		{
			name:    "changing the type of the column the owner changed",
			parser:  mysql,
			owner:   []string{"ALTER TABLE `users` MODIFY `email` varchar(320)"},
			planned: "ALTER TABLE `users` MODIFY `email` varchar(255)",
			want:    true,
		},
		{
			name:    "putting back the column the owner dropped",
			parser:  mysql,
			owner:   []string{"ALTER TABLE `users` DROP COLUMN `nickname`"},
			planned: "ALTER TABLE `users` ADD COLUMN `nickname` varchar(64), DROP COLUMN `bio`",
			want:    true,
		},
		{
			name:    "dropping the table the owner changed",
			parser:  mysql,
			owner:   []string{"ALTER TABLE `events` ADD INDEX `idx_created_at` (`created_at`)"},
			planned: "DROP TABLE `events`",
			want:    true,
		},
		{
			name:    "dropping any column of a table the owner created",
			parser:  mysql,
			owner:   []string{"CREATE TABLE `events` (`id` bigint, PRIMARY KEY (`id`))"},
			planned: "ALTER TABLE `events` DROP COLUMN `payload`",
			want:    true,
		},
		{
			name:    "dropping a column beside an expression index the owner added",
			parser:  mysql,
			owner:   []string{"ALTER TABLE `events` ADD INDEX `idx_day` ((DATE(`created_at`)))"},
			planned: "ALTER TABLE `events` DROP COLUMN `created_at`",
			want:    true,
		},
		{
			name:    "a statement that does not parse",
			parser:  mysql,
			owner:   []string{"ALTER TABLE `events` ADD INDEX `idx_created_at` (`created_at`)"},
			planned: "this is not SQL",
			want:    true,
		},
		{
			name:    "dropping the column the owner made invisible",
			parser:  mysql,
			owner:   []string{"ALTER TABLE `events` ALTER COLUMN `payload` SET INVISIBLE"},
			planned: "ALTER TABLE `events` DROP COLUMN `payload`",
			want:    true,
		},
		{
			name:    "postgres: dropping a column a partial index the owner added filters on",
			parser:  postgres,
			owner:   []string{"CREATE INDEX idx_live ON events (id) WHERE payload IS NOT NULL"},
			planned: "ALTER TABLE events DROP COLUMN payload",
			want:    true,
		},
		{
			name:    "postgres: dropping a column beside an index the owner added",
			parser:  postgres,
			owner:   []string{"CREATE INDEX CONCURRENTLY idx_created_at ON events (created_at)"},
			planned: "ALTER TABLE events DROP COLUMN payload",
			want:    false,
		},
		{
			name:    "postgres: dropping the column the owner added",
			parser:  postgres,
			owner:   []string{"ALTER TABLE events ADD COLUMN payload jsonb"},
			planned: "ALTER TABLE events DROP COLUMN payload",
			want:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := AttributedChangeData{Table: "events", Repository: "acme/orders", PullRequest: 190, OwnerChanges: ownerDid(t, tt.parser, tt.owner...)}
			assert.Equal(t, tt.want, UndoesAttributedWork(a, ReadPlannedChange(a.Table, tt.planned, tt.parser)))
		})
	}

	t.Run("an unresolved owner is always attributed", func(t *testing.T) {
		a := AttributedChangeData{Table: "events", Unresolved: true}
		assert.True(t, UndoesAttributedWork(a, ReadPlannedChange(a.Table, "ALTER TABLE `events` DROP COLUMN `payload`", mysql)))
	})
	t.Run("unknown owner changes are always attributed", func(t *testing.T) {
		a := AttributedChangeData{Table: "events", Repository: "acme/orders", PullRequest: 190}
		assert.True(t, UndoesAttributedWork(a, ReadPlannedChange(a.Table, "ALTER TABLE `events` DROP COLUMN `payload`", mysql)))
	})
	t.Run("no parser means every change is attributed", func(t *testing.T) {
		a := AttributedChangeData{Table: "events", Repository: "acme/orders", PullRequest: 190, OwnerChanges: &ddl.ObjectChanges{}}
		assert.True(t, UndoesAttributedWork(a, ReadPlannedChange(a.Table, "ALTER TABLE `events` DROP COLUMN `payload`", nil)))
	})
	t.Run("a change with no statement is attributed", func(t *testing.T) {
		a := AttributedChangeData{Table: "events", Repository: "acme/orders", PullRequest: 190, OwnerChanges: &ddl.ObjectChanges{}}
		assert.True(t, UndoesAttributedWork(a, ReadPlannedChange(a.Table, "  ", mysql)))
	})
}

// Two pull requests change the same table: an open one adds an index, and
// this one renames a column and drops that index. Only the finding that undoes
// the other pull request's index carries its attribution; the rename is this
// pull request's own and reads without a note.
func TestRenderPlanComment_AttributionRidesOnlyOnFindingsThatUndoTheOwnersWork(t *testing.T) {
	mysql, err := ddl.ParserForDialect(schema.DialectMySQL)
	require.NoError(t, err)

	data := unsafeConsentPlan("staging")
	data.Repository = "acme/orders"
	data.Changes = []KeyspaceChangeData{{
		Keyspace: "orders",
		Statements: []string{
			"ALTER TABLE `events` DROP COLUMN `payload`, ADD COLUMN `body` text",
			"ALTER TABLE `events` DROP INDEX `idx_created_at`",
		},
	}}
	data.UnsafeChanges = []UnsafeChangeData{
		{Table: "events", Reason: "DROP COLUMN discards the column's data", DDL: "ALTER TABLE `events` DROP COLUMN `payload`, ADD COLUMN `body` text"},
		{Table: "events", Reason: "DROP INDEX removes the index", DDL: "ALTER TABLE `events` DROP INDEX `idx_created_at`"},
	}
	data.AttributedChanges = []AttributedChangeData{{
		Table: "events", Repository: "acme/orders", PullRequest: 190,
		OwnerChanges: ownerDid(t, mysql, "ALTER TABLE `events` ADD INDEX `idx_created_at` (`created_at`)"),
	}}
	out := RenderPlanComment(*data)

	assert.Contains(t, out, "`events`: DROP INDEX removes the index (changed by open PR [#190](https://github.com/acme/orders/pull/190))\n")
	assert.Contains(t, out, "`events`: DROP COLUMN discards the column's data\n", "the rename is this PR's own change")
	assert.Equal(t, 1, strings.Count(out, "(changed by open PR"))
	assert.Contains(t, out, "a change another PR applied before merging shows up here as one to undo")
	assert.NotContains(t, out, "Check before applying", "the attribution rides on the finding that undoes the other PR's index")
}

// When no finding on an attributed table undoes the owner's work, nothing
// carries the note, so the attribution keeps its own section rather than
// vanishing: the section is the fallback that never drops a disclosure.
func TestRenderPlanComment_AttributionWithNoMatchingFindingKeepsItsSection(t *testing.T) {
	mysql, err := ddl.ParserForDialect(schema.DialectMySQL)
	require.NoError(t, err)

	data := unsafeConsentPlan("staging")
	data.Repository = "acme/orders"
	data.UnsafeChanges = []UnsafeChangeData{
		{Table: "events", Reason: "DROP COLUMN discards the column's data", DDL: "ALTER TABLE `events` DROP COLUMN `payload`"},
	}
	data.AttributedChanges = []AttributedChangeData{{
		Table: "events", Repository: "acme/orders", PullRequest: 190,
		OwnerChanges: ownerDid(t, mysql, "ALTER TABLE `events` ADD INDEX `idx_created_at` (`created_at`)"),
	}}
	out := RenderPlanComment(*data)

	assert.NotContains(t, out, "(changed by open PR")
	assert.NotContains(t, out, "shows up here as one to undo", "no finding carries a note to explain")
	assert.Contains(t, out, "**Check before applying**: 1 destructive change SchemaBot cannot attribute to this PR")
}

// One ALTER TABLE drops a column and an index. Another open pull request added
// only the index. The engine reports the dropped column as its own finding,
// and that finding concerns the column, not the index, so it reads without the
// other pull request's note. The attribution is not lost: no finding names
// the owner, so it keeps its own section.
func TestRenderPlanComment_CompoundAlterAttributesOnlyTheFindingOnTheOwnersObject(t *testing.T) {
	mysql, err := ddl.ParserForDialect(schema.DialectMySQL)
	require.NoError(t, err)

	stmt := "ALTER TABLE `events` DROP COLUMN `payload`, DROP INDEX `idx_created_at`"
	finding := "DROP COLUMN discards the column's data"
	data := unsafeConsentPlan("staging")
	data.Repository = "acme/orders"
	data.Changes = []KeyspaceChangeData{{Keyspace: "orders", Statements: []string{stmt}}}
	data.UnsafeChanges = []UnsafeChangeData{{
		Table: "events", Reason: finding, DDL: stmt,
		FindingColumns: map[string]string{finding: "payload"},
	}}
	data.AttributedChanges = []AttributedChangeData{{
		Table: "events", Repository: "acme/orders", PullRequest: 190,
		OwnerChanges: ownerDid(t, mysql, "ALTER TABLE `events` ADD INDEX `idx_created_at` (`created_at`)"),
	}}
	out := RenderPlanComment(*data)

	assert.Contains(t, out, "`events`: DROP COLUMN discards the column's data\n", "the dropped column is this PR's own change")
	assert.NotContains(t, out, "(changed by open PR")
	assert.Contains(t, out, "**Check before applying**: 1 destructive change SchemaBot cannot attribute to this PR")
	assert.Contains(t, out, "- `events`: changed by [acme/orders#190](https://github.com/acme/orders/pull/190), which is still open\n")

	t.Run("a finding that names no column is matched against the whole statement", func(t *testing.T) {
		data.UnsafeChanges[0].FindingColumns = nil
		out := RenderPlanComment(*data)
		assert.Contains(t, out, "`events`: DROP COLUMN discards the column's data (changed by open PR [#190](https://github.com/acme/orders/pull/190))\n")
		assert.NotContains(t, out, "Check before applying")
	})
}

// Two open pull requests each added a column to `users`, and this pull
// request's plan drops both in one statement. Each finding names the pull
// request that added its column, and since every owner is named by a finding,
// the attribution needs no section of its own.
func TestRenderPlanComment_EachFindingNamesTheOwnerOfItsColumn(t *testing.T) {
	mysql, err := ddl.ParserForDialect(schema.DialectMySQL)
	require.NoError(t, err)

	stmt := "ALTER TABLE `users` DROP COLUMN `email`, DROP COLUMN `nickname`"
	data := unsafeConsentPlan("staging")
	data.Repository = "acme/orders"
	data.Changes = []KeyspaceChangeData{{Keyspace: "orders", Statements: []string{stmt}}}
	data.UnsafeChanges = []UnsafeChangeData{{
		Table: "users", DDL: stmt,
		Reason:         "DROP COLUMN email discards its data; DROP COLUMN nickname discards its data",
		FindingColumns: map[string]string{"DROP COLUMN email discards its data": "email", "DROP COLUMN nickname discards its data": "nickname"},
	}}
	data.AttributedChanges = []AttributedChangeData{
		{Table: "users", Repository: "acme/orders", PullRequest: 3, OwnerChanges: ownerDid(t, mysql, "ALTER TABLE `users` ADD COLUMN `nickname` varchar(64)")},
		{Table: "users", Repository: "acme/orders", PullRequest: 2, OwnerChanges: ownerDid(t, mysql, "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")},
	}
	out := RenderPlanComment(*data)

	assert.Contains(t, out, "`users`: DROP COLUMN email discards its data (changed by open PR [#2](https://github.com/acme/orders/pull/2))\n")
	assert.Contains(t, out, "`users`: DROP COLUMN nickname discards its data (changed by open PR [#3](https://github.com/acme/orders/pull/3))\n")
	assert.NotContains(t, out, "Check before applying", "every owner is named by the finding that undoes its work")
}

// Two open pull requests changed the same column: one added it, the other
// changed its type. Dropping it undoes both, so the finding names both.
func TestRenderPlanComment_FindingThatUndoesTwoOwnersNamesBoth(t *testing.T) {
	mysql, err := ddl.ParserForDialect(schema.DialectMySQL)
	require.NoError(t, err)

	stmt := "ALTER TABLE `users` DROP COLUMN `email`"
	data := unsafeConsentPlan("staging")
	data.Repository = "acme/orders"
	data.Changes = []KeyspaceChangeData{{Keyspace: "orders", Statements: []string{stmt}}}
	data.UnsafeChanges = []UnsafeChangeData{{Table: "users", Reason: "DROP COLUMN discards the column's data", DDL: stmt}}
	data.AttributedChanges = []AttributedChangeData{
		{Table: "users", Repository: "acme/orders", PullRequest: 3, OwnerChanges: ownerDid(t, mysql, "ALTER TABLE `users` MODIFY `email` varchar(320)")},
		{Table: "users", Repository: "acme/orders", PullRequest: 2, OwnerChanges: ownerDid(t, mysql, "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")},
	}
	out := RenderPlanComment(*data)

	assert.Contains(t, out, "`users`: DROP COLUMN discards the column's data (changed by open PRs [#3](https://github.com/acme/orders/pull/3) and [#2](https://github.com/acme/orders/pull/2))\n")
	assert.NotContains(t, out, "Check before applying")

	t.Run("the section names every owner of the table", func(t *testing.T) {
		var sb strings.Builder
		writeAttributedChanges(&sb, data.AttributedChanges)
		assert.Contains(t, sb.String(), "**Check before applying**: 1 destructive change SchemaBot cannot attribute to this PR\n")
		assert.Contains(t, sb.String(), "- `users`: changed by [acme/orders#3](https://github.com/acme/orders/pull/3) and [acme/orders#2](https://github.com/acme/orders/pull/2), which are still open\n")
	})
}

// countingParser counts the statements it is asked to read.
type countingParser struct {
	ddl.StatementParser
	reads int
}

func (p *countingParser) ObjectChanges(stmt string) (ddl.ObjectChanges, error) {
	p.reads++
	return p.StatementParser.ObjectChanges(stmt)
}

// A render consults each finding once per owner, several times over, and a
// statement that does not parse would otherwise log a warning on every
// consultation. Each planned statement is read once per render, and a
// statement that does not parse still attributes every finding to every owner.
func TestUnsafeAttributionReadsEachPlannedStatementOnce(t *testing.T) {
	mysql, err := ddl.ParserForDialect(schema.DialectMySQL)
	require.NoError(t, err)
	parser := &countingParser{StatementParser: mysql}

	owners := []AttributedChangeData{
		{Table: "users", Repository: "acme/orders", PullRequest: 3, OwnerChanges: ownerDid(t, mysql, "ALTER TABLE `users` ADD COLUMN `nickname` varchar(64)")},
		{Table: "users", Repository: "acme/orders", PullRequest: 2, OwnerChanges: ownerDid(t, mysql, "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")},
	}
	notes := &unsafeAttribution{
		byTable:     map[string][]AttributedChangeData{"users": owners},
		parser:      parser,
		commentRepo: "acme/orders",
		planned:     map[string]*ddl.ObjectChanges{},
	}
	change := UnsafeChangeData{
		Table:  "users",
		DDL:    "ALTER TABLE `users` DROP COLUMN",
		Reason: "DROP COLUMN email discards its data; DROP COLUMN nickname discards its data",
	}

	for _, finding := range unsafeFindings(change) {
		assert.Equal(t, "changed by open PRs [#3](https://github.com/acme/orders/pull/3) and [#2](https://github.com/acme/orders/pull/2)", notes.noteFor(change, finding))
	}
	assert.True(t, notes.carriesNote(change))
	for _, a := range owners {
		assert.True(t, notes.namesOwner([]UnsafeChangeData{change}, a))
	}
	assert.Equal(t, 1, parser.reads, "the planned statement is read once however many findings and owners consult it")
}
