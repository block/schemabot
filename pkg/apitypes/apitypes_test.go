package apitypes

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlanResponse_UnsafeChanges(t *testing.T) {
	resp := &PlanResponse{
		Changes: []*SchemaChangeResponse{{
			Namespace: "testdb",
			TableChanges: []*TableChangeResponse{
				{TableName: "orders", DDL: "ALTER TABLE orders ADD COLUMN x INT", IsUnsafe: false},
				{TableName: "users", DDL: "DROP TABLE users", ChangeType: "drop", IsUnsafe: true, UnsafeReason: "DROP TABLE removes all data"},
				{TableName: "items", DDL: "ALTER TABLE items DROP INDEX idx", ChangeType: "alter", IsUnsafe: true, UnsafeReason: "DROP INDEX without making invisible first"},
			},
		}},
	}

	changes := resp.UnsafeChanges()
	require.Len(t, changes, 2)
	assert.Equal(t, "users", changes[0].Table)
	assert.Equal(t, "DROP TABLE removes all data", changes[0].Reason)
	assert.Equal(t, "drop", changes[0].ChangeType)
	assert.Equal(t, "items", changes[1].Table)
}

func TestPlanResponse_UnsafeChangesTreatsTableDropAsUnsafe(t *testing.T) {
	resp := &PlanResponse{
		Changes: []*SchemaChangeResponse{{
			Namespace: "testdb",
			TableChanges: []*TableChangeResponse{
				{TableName: "users", DDL: "DROP TABLE `users`", ChangeType: "drop"},
			},
		}},
	}

	changes := resp.UnsafeChanges()
	require.Len(t, changes, 1)
	assert.Equal(t, "users", changes[0].Table)
	assert.Equal(t, "DROP TABLE removes all data", changes[0].Reason)
	assert.Equal(t, "DROP TABLE `users`", changes[0].DDL)
	assert.Equal(t, "drop", changes[0].ChangeType)
}

func TestPlanResponse_UnsafeChangesToleratesNilEntries(t *testing.T) {
	resp := &PlanResponse{
		Changes: []*SchemaChangeResponse{
			nil,
			{
				Namespace: "testdb",
				TableChanges: []*TableChangeResponse{
					nil,
					{TableName: "users", DDL: "DROP TABLE `users`", ChangeType: "drop"},
				},
			},
		},
	}

	changes := resp.UnsafeChanges()
	require.Len(t, changes, 1)
	assert.Equal(t, "users", changes[0].Table)
	assert.Equal(t, "DROP TABLE removes all data", changes[0].Reason)
}

func TestPlanResponse_UnsafeChanges_None(t *testing.T) {
	resp := &PlanResponse{
		Changes: []*SchemaChangeResponse{{
			TableChanges: []*TableChangeResponse{
				{TableName: "orders", DDL: "ALTER TABLE orders ADD COLUMN x INT", IsUnsafe: false},
			},
		}},
	}

	assert.Empty(t, resp.UnsafeChanges())
}

func TestPlanResponse_LintViolations(t *testing.T) {
	resp := &PlanResponse{
		LintResults: []*LintViolationResponse{
			{Message: "DROP TABLE", Table: "users", Linter: "unsafe", Severity: "error"},
			{Message: "invisible index", Table: "items", Linter: "invisible_index_before_drop", Severity: "error"},
			{Message: "INT PK", Table: "orders", Linter: "primary_key", Severity: "warning"},
			{Message: "charset", Table: "orders", Linter: "allow_charset", Severity: "info"},
		},
	}

	warnings := resp.LintNonErrors()
	require.Len(t, warnings, 2)
	assert.Equal(t, "primary_key", warnings[0].Linter)
	assert.Equal(t, "allow_charset", warnings[1].Linter)
}

func TestPlanResponse_LintErrors(t *testing.T) {
	resp := &PlanResponse{
		LintResults: []*LintViolationResponse{
			{Message: "DROP TABLE", Table: "users", Linter: "unsafe", Severity: "error"},
			{Message: "invisible index", Table: "items", Linter: "invisible_index_before_drop", Severity: "error"},
			{Message: "INT PK", Table: "orders", Linter: "primary_key", Severity: "warning"},
		},
	}

	errors := resp.LintErrors()
	require.Len(t, errors, 2)
	assert.Equal(t, "unsafe", errors[0].Linter)
	assert.Equal(t, "invisible_index_before_drop", errors[1].Linter)
}

func TestPlanResponse_LintViolations_Empty(t *testing.T) {
	resp := &PlanResponse{}
	assert.Empty(t, resp.LintNonErrors())
	assert.Empty(t, resp.LintErrors())
}

func TestPlanResponse_HasErrors(t *testing.T) {
	resp := &PlanResponse{
		LintResults: []*LintViolationResponse{
			{Severity: "warning"},
		},
	}
	assert.False(t, resp.HasErrors())

	resp.LintResults = append(resp.LintResults, &LintViolationResponse{Severity: "error"})
	assert.True(t, resp.HasErrors())
}

func TestPlanResponse_HasChanges(t *testing.T) {
	tests := []struct {
		name string
		resp *PlanResponse
		want bool
	}{
		{
			name: "no changes",
			resp: &PlanResponse{Changes: []*SchemaChangeResponse{{Namespace: "testdb"}}},
			want: false,
		},
		{
			name: "table changes only",
			resp: &PlanResponse{Changes: []*SchemaChangeResponse{{
				Namespace:    "testdb",
				TableChanges: []*TableChangeResponse{{TableName: "users", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"}},
			}}},
			want: true,
		},
		{
			name: "vschema diff only",
			resp: &PlanResponse{Changes: []*SchemaChangeResponse{{
				Namespace: "boardgames_sharded",
				Metadata:  map[string]string{VSchemaDiffMetadataKey: "--- current\n+++ new\n+    \"xxhash\": {\"type\": \"xxhash\"}"},
			}}},
			want: true,
		},
		{
			name: "vschema changed flag only",
			resp: &PlanResponse{Changes: []*SchemaChangeResponse{{
				Namespace: "boardgames_sharded",
				Metadata:  map[string]string{VSchemaChangedMetadataKey: "true"},
			}}},
			want: true,
		},
		{
			name: "vschema changed flag not true",
			resp: &PlanResponse{Changes: []*SchemaChangeResponse{{
				Namespace: "boardgames_sharded",
				Metadata:  map[string]string{VSchemaChangedMetadataKey: "false"},
			}}},
			want: false,
		},
		{
			name: "table changes and vschema",
			resp: &PlanResponse{Changes: []*SchemaChangeResponse{{
				Namespace:    "boardgames_sharded",
				TableChanges: []*TableChangeResponse{{TableName: "games", DDL: "ALTER TABLE `games` ADD COLUMN `rating` int"}},
				Metadata:     map[string]string{VSchemaDiffMetadataKey: "+ xxhash"},
			}}},
			want: true,
		},
		{
			name: "nil change entry",
			resp: &PlanResponse{Changes: []*SchemaChangeResponse{nil}},
			want: false,
		},
		{
			name: "empty plan",
			resp: &PlanResponse{},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.resp.HasChanges())
		})
	}
}
