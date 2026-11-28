package tern

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
)

const createDocumentsWithRowSecurity = `CREATE TABLE public.documents (id bigint NOT NULL, owner text);
ALTER TABLE public.documents ENABLE ROW LEVEL SECURITY;
CREATE POLICY own_rows ON public.documents AS PERMISSIVE FOR ALL TO PUBLIC USING ((owner = CURRENT_USER))`

// The planned create of a new table with its row security keys the same in
// any spelling of the same statements, keys apart when a policy differs, and
// keeps drift closed against a statement no DDL vocabulary covers.
func TestCanonicalDDLForDrift_CreateWithRowSecurity(t *testing.T) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)

	planned, err := canonicalDDLForDrift(parser, createDocumentsWithRowSecurity)
	require.NoError(t, err)
	assert.Contains(t, planned, "CREATE TABLE")
	assert.Contains(t, planned, "ENABLE ROW LEVEL SECURITY")
	assert.Contains(t, planned, "own_rows")

	respelled, err := canonicalDDLForDrift(parser, `create table public.documents (id bigint not null, owner text);
alter table public.documents enable row level security;
create policy own_rows on public.documents as permissive for all to public using ((owner = current_user));`)
	require.NoError(t, err)
	assert.Equal(t, planned, respelled)

	otherPolicy, err := canonicalDDLForDrift(parser, `CREATE TABLE public.documents (id bigint NOT NULL, owner text);
ALTER TABLE public.documents ENABLE ROW LEVEL SECURITY;
CREATE POLICY own_rows ON public.documents AS PERMISSIVE FOR SELECT TO PUBLIC USING ((owner = CURRENT_USER))`)
	require.NoError(t, err)
	assert.NotEqual(t, planned, otherPolicy)

	_, err = canonicalDDLForDrift(parser, createDocumentsWithRowSecurity+";\nINSERT INTO public.documents VALUES (1, 'x')")
	require.Error(t, err, "a trailing statement outside DDL does not hide behind the create")
}

// The sequential drive hands the engine the reviewed files for every task
// that declares row security: an atomic row security change and the create of
// a new table with its row security. Other PostgreSQL DDL, and other engines,
// load no files.
func TestTaskNeedsRowSecurityFiles(t *testing.T) {
	postgres := &LocalClient{config: LocalConfig{Type: storage.DatabaseTypePostgres}}
	for name, tc := range map[string]struct {
		client *LocalClient
		ddl    string
		want   bool
	}{
		"atomic row security change": {postgres, "ALTER TABLE public.documents ENABLE ROW LEVEL SECURITY", true},
		"create with row security":   {postgres, createDocumentsWithRowSecurity, true},
		"plain alter":                {postgres, "ALTER TABLE public.documents ADD COLUMN summary text", false},
		"plain create":               {postgres, "CREATE TABLE public.notes (id bigint NOT NULL)", false},
		"other engine":               {&LocalClient{config: LocalConfig{Type: storage.DatabaseTypeMySQL}}, "ALTER TABLE documents ADD COLUMN summary text", false},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := tc.client.taskNeedsRowSecurityFiles(&storage.Task{TaskIdentifier: "task-1", DDL: tc.ddl})
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
