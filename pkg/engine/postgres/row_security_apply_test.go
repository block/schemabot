package postgres

import (
	"testing"

	pgstatement "github.com/block/pg-sprite/pkg/statement"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
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
