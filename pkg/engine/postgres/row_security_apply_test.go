package postgres

import (
	"testing"

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
	for _, tt := range []struct {
		name, namespace, table, definition string
		duplicate, deferCutover            bool
		wantError                          string
	}{
		{name: "valid", namespace: "public", table: "documents", definition: desired},
		{name: "wrong namespace", namespace: "other", table: "documents", definition: desired, wantError: "target differs"},
		{name: "wrong table", namespace: "public", table: "other", definition: desired, wantError: "target differs"},
		{name: "table only file", namespace: "public", table: "documents", definition: "CREATE TABLE documents (id bigint PRIMARY KEY)", wantError: "declaration for table"},
		{name: "duplicate file", namespace: "public", table: "documents", definition: desired, duplicate: true, wantError: "multiple row security declarations"},
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
			req.SchemaFiles = schema.SchemaFiles{tt.namespace: {Files: files}}
			if tt.deferCutover {
				req.Options = map[string]string{"defer_cutover": "true"}
			}
			change, err := validateOptimisticApply(req)
			if tt.wantError != "" {
				require.ErrorContains(t, err, tt.wantError)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, change.rowSecurity)
			require.Len(t, change.reviewedSecurity, 2)
		})
	}
}
