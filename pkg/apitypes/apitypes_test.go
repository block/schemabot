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
