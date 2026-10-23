package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
)

// A schema file the pull request adds is absent at both bases, so its change
// is the file's own statements. Reformatting the file or editing its comments
// keeps the approval; any change to what the statements build does not. The
// comparison reads only the parse tree, so an engine with no target
// credentials answers it.
func TestEngineCompareSchemaFileChangeAddedFile(t *testing.T) {
	const approved = "CREATE TABLE overrides (\n" +
		"  id bigint PRIMARY KEY,\n" +
		"  program_token text NOT NULL,\n" +
		"  created_at timestamptz NOT NULL DEFAULT now()\n" +
		");\n" +
		"CREATE INDEX idx_program_token ON overrides (program_token);\n"
	tests := []struct {
		name       string
		head       string
		wantSame   bool
		wantReason string
	}{
		{
			name: "a reformatted file with a comment added",
			head: "-- Per-program overrides.\nCREATE TABLE overrides (id bigint PRIMARY KEY, program_token text NOT NULL, created_at timestamptz NOT NULL DEFAULT now());\n" +
				"create index idx_program_token on overrides (program_token);",
			wantSame: true,
		},
		{
			name: "a column whose type changes at the head",
			head: "CREATE TABLE overrides (id bigint PRIMARY KEY, program_token text NOT NULL, created_at timestamp NOT NULL DEFAULT now());\n" +
				"CREATE INDEX idx_program_token ON overrides (program_token);",
			wantReason: "statement 1 of the file differs between the approved commit and the head",
		},
		{
			name: "columns the head reorders",
			head: "CREATE TABLE overrides (id bigint PRIMARY KEY, created_at timestamptz NOT NULL DEFAULT now(), program_token text NOT NULL);\n" +
				"CREATE INDEX idx_program_token ON overrides (program_token);",
			wantReason: "statement 1 of the file differs between the approved commit and the head",
		},
		{
			name: "an index the head makes unique",
			head: "CREATE TABLE overrides (id bigint PRIMARY KEY, program_token text NOT NULL, created_at timestamptz NOT NULL DEFAULT now());\n" +
				"CREATE UNIQUE INDEX idx_program_token ON overrides (program_token);",
			wantReason: "statement 2 of the file differs between the approved commit and the head",
		},
		{
			name:       "an index the head drops",
			head:       "CREATE TABLE overrides (id bigint PRIMARY KEY, program_token text NOT NULL, created_at timestamptz NOT NULL DEFAULT now());",
			wantReason: "the file holds 2 statements at the approved commit but 1 at the head",
		},
	}
	e := &Engine{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := e.CompareSchemaFileChange(t.Context(), &ternv1.CompareSchemaFileChangeRequest{Approved: approved, Head: tt.head})
			require.NoError(t, err)
			assert.Equal(t, tt.wantSame, resp.GetSame())
			assert.Equal(t, tt.wantReason, resp.GetReason())
		})
	}
}

// A file absent at only one base has no single change to compare, and one
// PostgreSQL cannot parse proves nothing; both are errors the caller counts
// as changed.
func TestEngineCompareSchemaFileChangeAddedFileCannotCompare(t *testing.T) {
	const table = "CREATE TABLE t (id bigint PRIMARY KEY);"
	tests := []struct {
		name    string
		req     *ternv1.CompareSchemaFileChangeRequest
		wantErr string
	}{
		{
			name:    "absent at the approved commit's base only",
			req:     &ternv1.CompareSchemaFileChangeRequest{Approved: table, HeadBase: table, Head: table},
			wantErr: "the file is absent at one base but not the other",
		},
		{
			name:    "unparseable at the head",
			req:     &ternv1.CompareSchemaFileChangeRequest{Approved: table, Head: "CREATE TABLE t (id bigint PRIMARY KEY,,);"},
			wantErr: "read the file at the head: parse",
		},
		{
			name:    "only a comment at the approved commit",
			req:     &ternv1.CompareSchemaFileChangeRequest{Approved: "-- nothing here\n", Head: table},
			wantErr: "read the file at the approved commit: the file holds no statements",
		},
	}
	e := &Engine{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := e.CompareSchemaFileChange(t.Context(), tt.req)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
			assert.Nil(t, resp)
		})
	}
}
