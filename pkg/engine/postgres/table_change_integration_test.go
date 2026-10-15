//go:build integration

package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/testutil"
)

// A pull request edits orders.sql and is approved; the default branch then
// edits the same file and the PR is rebased, so the head's file combines both
// edits. The target runs all four versions and keeps the approval only when
// the PR's edit is the same schema change on the newer base and the head does
// not override what the default branch changed. Running the files leaves
// nothing behind on the target.
func TestEngineCompareSchemaFileChange(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "compare_test")
	eng := NewForTarget(0, 0, "compare_test", &engine.Credentials{DSN: dsn})
	orders := func(body string) string { return "CREATE TABLE orders (" + body + ");" }

	tests := []struct {
		name                                   string
		approvedBase, approved, headBase, head string
		wantSame                               bool
		wantReason                             string
	}{
		{
			name:         "a column the default branch added beside the PR's column",
			approvedBase: orders("id bigint PRIMARY KEY"),
			approved:     orders("id bigint PRIMARY KEY, note text"),
			headBase:     orders("id bigint PRIMARY KEY, total bigint NOT NULL DEFAULT 0"),
			head:         orders("id bigint PRIMARY KEY, total bigint NOT NULL DEFAULT 0, note text"),
			wantSame:     true,
		},
		{
			name:         "a check and an index the default branch added beside the PR's column",
			approvedBase: orders("id bigint PRIMARY KEY, total bigint"),
			approved:     orders("id bigint PRIMARY KEY, total bigint, note text"),
			headBase:     orders("id bigint PRIMARY KEY, total bigint, CONSTRAINT total_positive CHECK (total > 0)") + "\nCREATE INDEX orders_total_idx ON orders (total);",
			head:         orders("id bigint PRIMARY KEY, total bigint, note text, CONSTRAINT total_positive CHECK (total > 0)") + "\nCREATE INDEX orders_total_idx ON orders (total);",
			wantSame:     true,
		},
		{
			name:         "a file the PR reformatted on an unchanged base",
			approvedBase: orders("id bigint PRIMARY KEY"),
			approved:     "create table orders (\n  id bigint primary key,\n  note text\n);",
			headBase:     orders("id bigint PRIMARY KEY"),
			head:         orders("id bigint PRIMARY KEY, note text"),
			wantSame:     true,
		},
		{
			name:         "a column the default branch made NOT NULL and the PR widened",
			approvedBase: orders("id bigint PRIMARY KEY, total integer"),
			approved:     orders("id bigint PRIMARY KEY, total bigint"),
			headBase:     orders("id bigint PRIMARY KEY, total integer NOT NULL"),
			head:         orders("id bigint PRIMARY KEY, total bigint NOT NULL"),
			wantReason:   "both the approved commit and the base changed column total",
		},
		{
			name:         "a head that drops the column the default branch added",
			approvedBase: orders("id bigint PRIMARY KEY"),
			approved:     orders("id bigint PRIMARY KEY, note text"),
			headBase:     orders("id bigint PRIMARY KEY, total bigint"),
			head:         orders("id bigint PRIMARY KEY, note text"),
			wantReason:   "the base changed column total since the approved commit, and the head replaces it with another version",
		},
		{
			name:         "a head that declares the PR's column differently",
			approvedBase: orders("id bigint PRIMARY KEY"),
			approved:     orders("id bigint PRIMARY KEY, note text"),
			headBase:     orders("id bigint PRIMARY KEY, total bigint"),
			head:         orders("id bigint PRIMARY KEY, total bigint, note varchar(10)"),
			wantReason:   "the head declares column note differently from the approved commit",
		},
		{
			name:         "a table property the head changes beyond the PR's edit",
			approvedBase: orders("id bigint PRIMARY KEY"),
			approved:     orders("id bigint PRIMARY KEY, note text"),
			headBase:     orders("id bigint PRIMARY KEY, total bigint"),
			head:         "CREATE UNLOGGED TABLE orders (id bigint PRIMARY KEY, total bigint, note text);",
			wantReason:   "the head declares table clause relation differently from the approved commit",
		},
		{
			name:         "a storage parameter the default branch added beside the PR's column",
			approvedBase: orders("id bigint PRIMARY KEY"),
			approved:     orders("id bigint PRIMARY KEY, note text"),
			headBase:     "CREATE TABLE orders (id bigint PRIMARY KEY) WITH (fillfactor = 70);",
			head:         "CREATE TABLE orders (id bigint PRIMARY KEY, note text) WITH (fillfactor = 70);",
			wantSame:     true,
		},
		{
			name:         "a storage parameter the head adds beyond the PR's edit",
			approvedBase: orders("id bigint PRIMARY KEY"),
			approved:     orders("id bigint PRIMARY KEY, note text"),
			headBase:     orders("id bigint PRIMARY KEY, total bigint"),
			head:         "CREATE TABLE orders (id bigint PRIMARY KEY, total bigint, note text) WITH (autovacuum_enabled = false);",
			wantReason:   "the head declares table clause options differently from the approved commit",
		},
		{
			name:         "a tablespace the head adds beyond the PR's edit",
			approvedBase: orders("id bigint PRIMARY KEY"),
			approved:     orders("id bigint PRIMARY KEY, note text"),
			headBase:     orders("id bigint PRIMARY KEY, total bigint"),
			head:         "CREATE TABLE orders (id bigint PRIMARY KEY, total bigint, note text) TABLESPACE pg_default;",
			wantReason:   "the head declares table clause tablespacename differently from the approved commit",
		},
		{
			name:         "identity sequence options the head changes beyond the PR's edit",
			approvedBase: orders("id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY"),
			approved:     orders("id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, note text"),
			headBase:     orders("id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, total bigint"),
			head:         orders("id bigint GENERATED ALWAYS AS IDENTITY (START WITH 1000) PRIMARY KEY, total bigint, note text"),
			wantReason:   "the head declares column id differently from the approved commit",
		},
		{
			name:         "a column storage mode the head adds to the PR's column",
			approvedBase: orders("id bigint PRIMARY KEY"),
			approved:     orders("id bigint PRIMARY KEY, note text"),
			headBase:     orders("id bigint PRIMARY KEY, total bigint"),
			head:         orders("id bigint PRIMARY KEY, total bigint, note text STORAGE EXTERNAL"),
			wantReason:   "the head declares column note differently from the approved commit",
		},
		{
			name:         "index storage parameters the head changes on the default branch's index",
			approvedBase: orders("id bigint PRIMARY KEY, total bigint"),
			approved:     orders("id bigint PRIMARY KEY, total bigint, note text"),
			headBase:     orders("id bigint PRIMARY KEY, total bigint") + "\nCREATE INDEX orders_total_idx ON orders (total);",
			head:         orders("id bigint PRIMARY KEY, total bigint, note text") + "\nCREATE INDEX orders_total_idx ON orders (total) WITH (fillfactor = 50);",
			wantReason:   "the base changed index orders_total_idx since the approved commit, and the head replaces it with another version",
		},
		{
			name:         "unnamed checks the PR and the default branch both add, which PostgreSQL names by position",
			approvedBase: orders("id bigint PRIMARY KEY, total bigint"),
			approved:     orders("id bigint PRIMARY KEY, total bigint, CHECK (total > 0)"),
			headBase:     orders("id bigint PRIMARY KEY, total bigint, CHECK (total < 1000000)"),
			head:         orders("id bigint PRIMARY KEY, total bigint, CHECK (total < 1000000), CHECK (total > 0)"),
			wantReason:   "table orders: the approved commit generates",
		},
		{
			name:         "a head that reorders two columns the PR changed",
			approvedBase: orders("id bigint PRIMARY KEY, x integer, y integer"),
			approved:     orders("id bigint PRIMARY KEY, x bigint, y bigint"),
			headBase:     orders("id bigint PRIMARY KEY, x integer, y integer, note text"),
			head:         orders("id bigint PRIMARY KEY, y bigint, x bigint, note text"),
			wantReason:   "table orders: the approved commit generates",
		},
		{
			name:         "a file that declares another table at the head",
			approvedBase: orders("id bigint PRIMARY KEY"),
			approved:     orders("id bigint PRIMARY KEY, note text"),
			headBase:     orders("id bigint PRIMARY KEY"),
			head:         "CREATE TABLE invoices (id bigint PRIMARY KEY, note text);",
			wantReason:   "the file declares table orders at the approved commit's base but invoices at the head",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := eng.CompareSchemaFileChange(t.Context(), &ternv1.CompareSchemaFileChangeRequest{
				Database: "compare_test", Type: "postgres", Environment: "test",
				ApprovedBase: tt.approvedBase, Approved: tt.approved, HeadBase: tt.headBase, Head: tt.head,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantSame, resp.GetSame())
			if tt.wantSame {
				assert.Empty(t, resp.GetReason())
			} else {
				assert.Contains(t, resp.GetReason(), tt.wantReason)
			}
		})
	}

	var tables int
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE c.relname IN ('orders', 'invoices') AND n.nspname NOT IN ('pg_catalog', 'information_schema')`).Scan(&tables))
	assert.Zero(t, tables, "running the files on the target leaves no table behind")
}

// A file the comparison cannot run proves nothing, so it is an error rather
// than a verdict, and the review gate treats the file as changed.
func TestEngineCompareSchemaFileChangeRejectsFilesItCannotRun(t *testing.T) {
	dsn, _ := testutil.StartPostgres(t, "compare_reject_test")
	eng := NewForTarget(0, 0, "compare_reject_test", &engine.Credentials{DSN: dsn})
	base := "CREATE TABLE orders (id bigint PRIMARY KEY);"
	approved := "CREATE TABLE orders (id bigint PRIMARY KEY, note text);"

	tests := []struct {
		name string
		// approved defaults to the file with the PR's column; a case whose
		// files merge sets it to its head so the comparison reaches the
		// target.
		approved string
		head     string
		wantErr  string
	}{
		{
			name:    "a file that does not parse",
			head:    "CREATE TABLE orders (id bigint PRIMARY KEY,",
			wantErr: "read the file at the head",
		},
		{
			name:    "a file with row security",
			head:    approved + "\nALTER TABLE orders ENABLE ROW LEVEL SECURITY;\nCREATE POLICY orders_read ON orders FOR SELECT USING (true);",
			wantErr: "row security",
		},
		{
			name:     "a file the target rejects",
			approved: "CREATE TABLE orders (id bigint PRIMARY KEY, note no_such_type);",
			head:     "CREATE TABLE orders (id bigint PRIMARY KEY, note no_such_type);",
			wantErr:  "run table orders at the approved commit",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			caseApproved := approved
			if tt.approved != "" {
				caseApproved = tt.approved
			}
			resp, err := eng.CompareSchemaFileChange(t.Context(), &ternv1.CompareSchemaFileChangeRequest{
				Database: "compare_reject_test", Type: "postgres", Environment: "test",
				ApprovedBase: base, Approved: caseApproved, HeadBase: base, Head: tt.head,
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
			assert.Nil(t, resp)
		})
	}
}
