package postgres

import (
	"errors"
	"fmt"
	"testing"

	"github.com/block/pg-sprite/pkg/executor"
	"github.com/block/pg-sprite/pkg/schemadiff"
	pgstatement "github.com/block/pg-sprite/pkg/statement"
	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/ui"
	"github.com/stretchr/testify/require"
)

func TestValidateRowSecurityApply(t *testing.T) {
	const operation = `
 DROP POLICY readers ON public.documents;
 CREATE POLICY readers ON public.documents FOR SELECT USING (id = 2);
 `
	const desired = `
 CREATE TABLE documents (id bigint PRIMARY KEY);
 ALTER TABLE documents ENABLE ROW LEVEL SECURITY;
 CREATE POLICY readers ON documents FOR SELECT USING (id = 2);
 `
	// Every reviewed file in the namespace is admitted, so an ordinary table's
	// file beside the row security declaration must pass its own admission.
	const ordinaryCompanion = `
 CREATE TABLE orders (
     id bigint PRIMARY KEY,
     placed_at timestamptz NOT NULL
 );
 CREATE INDEX orders_placed_at_idx ON orders (placed_at);
 `
	// A foreign key to an unqualified table is admitted by ordinary
	// admission, so this companion passes the apply check.
	const foreignKeyCompanion = `
 CREATE TABLE orders (
     id bigint PRIMARY KEY,
     document_id bigint REFERENCES documents (id)
 );
 `
	// A schema-qualified reference is refused by ordinary admission, so this
	// companion fails the apply check, and the error names its file.
	const qualifiedReferenceCompanion = `
 CREATE TABLE orders (
     id bigint PRIMARY KEY,
     document_id bigint REFERENCES other.documents (id)
 );
 `
	// Another table's row security declaration is the ordinary case for a
	// namespace with row security on several tables; only the declaration for
	// the apply's table counts.
	const rowSecurityCompanion = `
 CREATE TABLE orders (id bigint PRIMARY KEY);
 ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
 CREATE POLICY readers ON orders FOR SELECT USING (id = 2);
 `
	for _, tt := range []struct {
		name, namespace, table, definition string
		companion                          string
		duplicate, deferCutover            bool
		wantError                          string
		wantErrorIs                        error
	}{
		{name: "valid", namespace: "public", table: "documents", definition: desired},
		{name: "wrong namespace", namespace: "other", table: "documents", definition: desired, wantError: "target differs"},
		{name: "wrong table", namespace: "public", table: "other", definition: desired, wantError: "target differs"},
		{name: "table only file", namespace: "public", table: "documents", definition: "CREATE TABLE documents (id bigint PRIMARY KEY)", wantError: "declaration for table"},
		{name: "duplicate file", namespace: "public", table: "documents", definition: desired, duplicate: true, wantError: "multiple row security declarations"},
		{name: "admitted ordinary companion", namespace: "public", table: "documents", definition: desired, companion: ordinaryCompanion},
		{name: "admitted foreign-key companion", namespace: "public", table: "documents", definition: desired, companion: foreignKeyCompanion},
		{name: "unsupported ordinary companion", namespace: "public", table: "documents", definition: desired, companion: qualifiedReferenceCompanion,
			wantError: `parse schema file "orders.sql"`, wantErrorIs: pgstatement.ErrQualifiedName},
		{name: "other table's row security companion", namespace: "public", table: "documents", definition: desired, companion: rowSecurityCompanion},
		{name: "deferred cutover", namespace: "public", table: "documents", definition: desired, deferCutover: true, wantError: "do not support deferred cutover"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := &engine.ApplyRequest{
				Database: "app", Credentials: &engine.Credentials{DSN: "postgres://localhost/app"},
				Changes: []engine.SchemaChange{{Namespace: tt.namespace, TableChanges: []engine.TableChange{{Table: tt.table, DDL: operation}}}},
			}
			req.Changes[0].Namespace = tt.namespace
			files := map[string]string{"documents.sql": tt.definition}
			if tt.duplicate {
				files["duplicate.sql"] = desired
			}
			if tt.companion != "" {
				files["orders.sql"] = tt.companion
			}
			req.SchemaFiles = schema.SchemaFiles{tt.namespace: {Files: files}}
			if tt.deferCutover {
				req.Options = map[string]string{"defer_cutover": "true"}
			}
			change, err := validateOptimisticApply(req)
			if tt.wantError != "" {
				require.ErrorContains(t, err, tt.wantError)
				if tt.wantErrorIs != nil {
					require.ErrorIs(t, err, tt.wantErrorIs)
				}
				return
			}
			require.NoError(t, err)
			require.NotNil(t, change.rowSecurity)
			require.Equal(t, tt.table, change.rowSecurity.Table())
			require.Len(t, change.reviewedSecurity, 2)
		})
	}
}

// A table shape the row security path refuses becomes a plan refusal that
// names the table and the shape. A missing privilege, which a grant fixes,
// and an error that names no shape stay errors.
func TestUnsupportedRowSecurityShape(t *testing.T) {
	live := schemadiff.Model{Table: "events"}
	unlogged := fmt.Errorf("admit row security target: %w: render table %q: %w", executor.ErrRowSecurityRefused, "events", schemadiff.ErrUnrenderableUnlogged)
	refusal, ok := unsupportedRowSecurityShape("public", live, unlogged)
	require.True(t, ok)
	var refused *engine.PlanRefusedError
	require.ErrorAs(t, refusal, &refused)
	assert.Equal(t, `SchemaBot does not manage row security on table "events" yet, because it is unlogged; remove the row security statements from its schema file, then plan again`, refusal.Error())

	collation := fmt.Errorf("%w: %w", executor.ErrRowSecurityRefused, schemadiff.ErrUnrenderableCollation)
	refusal, ok = unsupportedRowSecurityShape("public", live, collation)
	require.True(t, ok)
	assert.Contains(t, refusal.Error(), `on table "events" yet, because it has a column with an explicit collation; remove`)

	wrapped := fmt.Errorf("%w: %w", executor.ErrRowSecurityRefused, schemadiff.ErrUnrenderableUnlogged)
	assert.Equal(t, "of a part of its definition SchemaBot cannot compare yet", rowSecurityShapePhrase(errors.New("a shape added later")))
	assert.Equal(t, "it is unlogged", rowSecurityShapePhrase(wrapped))

	_, ok = unsupportedRowSecurityShape("public", live, fmt.Errorf("%w: %w: %w", executor.ErrRowSecurityRefused, executor.ErrRowSecurityPrivileges, schemadiff.ErrUnrenderableUnlogged))
	assert.False(t, ok, "a missing privilege is fixed by a grant, not by the file")
	_, ok = unsupportedRowSecurityShape("public", live, fmt.Errorf("%w: mixed", executor.ErrRowSecurityRefused))
	assert.False(t, ok, "a refusal that names no table shape is not this one")
	_, ok = unsupportedRowSecurityShape("public", live, errors.New("connection reset"))
	assert.False(t, ok)
}

// A table that already has row security keeps it whatever the file says,
// and a file that stops declaring row security SchemaBot applied is
// refused, so the refusal asks for statements that match the table instead
// of their removal.
func TestUnsupportedRowSecurityShapeOnATableWithRowSecurity(t *testing.T) {
	unlogged := fmt.Errorf("%w: %w", executor.ErrRowSecurityRefused, schemadiff.ErrUnrenderableUnlogged)
	for name, rs := range map[string]schemadiff.RowSecurity{
		"enabled":  {Enabled: true},
		"forced":   {Forced: true},
		"a policy": {Policies: []schemadiff.Policy{{Name: "readers", Permissive: true}}},
	} {
		refusal, ok := unsupportedRowSecurityShape("public", schemadiff.Model{Table: "events", RowSecurity: rs}, unlogged)
		require.True(t, ok, name)
		assert.Equal(t, `SchemaBot does not manage row security on table "events" yet, because it is unlogged; make the row security statements in its schema file match the table's current settings and policies, then plan again`, refusal.Error(), name)
	}
}

// Row security that is on denies a command unless a permissive policy
// grants it, for that command or for ALL; restrictive policies only narrow
// what a permissive one grants. The plan names the commands denied and what
// each does to the application, as one finding.
func TestDeniedCommands(t *testing.T) {
	const denyAll = "after this change row security is on with no policy that grants access, so every role subject to it reads no rows, its inserts are refused, and its updates and deletes change no rows: declare a permissive policy in the same file"
	for name, tc := range map[string]struct {
		rs   schemadiff.RowSecurity
		want string
	}{
		"row security off": {rs: schemadiff.RowSecurity{}},
		"no policy":        {rs: schemadiff.RowSecurity{Enabled: true}, want: denyAll},
		"only a restrictive policy": {rs: schemadiff.RowSecurity{Enabled: true, Policies: []schemadiff.Policy{
			{Name: "narrow", Command: schemadiff.PolicyAll}}}, want: denyAll},
		"a permissive policy for ALL": {rs: schemadiff.RowSecurity{Enabled: true, Policies: []schemadiff.Policy{
			{Name: "narrow", Command: schemadiff.PolicySelect}, {Name: "owners", Command: schemadiff.PolicyAll, Permissive: true}}}},
		"a permissive policy for SELECT": {rs: schemadiff.RowSecurity{Enabled: true, Policies: []schemadiff.Policy{
			{Name: "readers", Command: schemadiff.PolicySelect, Permissive: true}}},
			want: "after this change no permissive policy grants INSERT, UPDATE, or DELETE, so every role subject to row security has its inserts refused, changes no rows with its updates, and changes no rows with its deletes: if the application runs them, declare a permissive policy for them in the same file"},
		"permissive policies for every command but DELETE": {rs: schemadiff.RowSecurity{Enabled: true, Policies: []schemadiff.Policy{
			{Name: "readers", Command: schemadiff.PolicySelect, Permissive: true},
			{Name: "writers", Command: schemadiff.PolicyInsert, Permissive: true},
			{Name: "editors", Command: schemadiff.PolicyUpdate, Permissive: true}}},
			want: "after this change no permissive policy grants DELETE, so every role subject to row security changes no rows with its deletes: if the application runs it, declare a permissive policy for it in the same file"},
	} {
		denied := deniedCommands(tc.rs)
		if tc.want == "" {
			assert.Empty(t, denied, name)
			continue
		}
		reason := denyReason(denied)
		assert.Equal(t, tc.want, reason, name)
		assert.Len(t, ui.LintReasons(reason), 1, "%s: the warning and its fix are one finding", name)
	}
}

// Each table shape the row security path cannot manage reads as what it is
// about the table, and a foreign key refusal with no key to list, such as a
// table the target lacks, names the shape alone rather than an empty list.
func TestRowSecurityShapePhrases(t *testing.T) {
	for shape, want := range map[error]string{
		schemadiff.ErrUnrenderableDefault:    "it has a column whose default draws from a sequence other than the one a serial column declares",
		schemadiff.ErrMultipleOwnedSequences: "it has a column whose default uses more than one sequence the column owns",
		schemadiff.ErrUnrenderablePartition:  "it is a partition of a partitioned table",
	} {
		assert.Equal(t, want, rowSecurityShapePhrase(fmt.Errorf("%w: %w", executor.ErrRowSecurityRefused, shape)))
	}
	refusal, ok := unsupportedRowSecurityShape("public", schemadiff.Model{Table: "cards"},
		fmt.Errorf("%w: %w", executor.ErrRowSecurityRefused, schemadiff.ErrUnrenderableForeignKey))
	require.True(t, ok)
	assert.Equal(t, `SchemaBot does not manage row security on table "cards" yet, because it is in a foreign key relationship; remove the row security statements from its schema file, then plan again`, refusal.Error())
}
