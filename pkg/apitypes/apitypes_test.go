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

// A blocked verdict anywhere in the plan — namespace-level or per-shard —
// marks the plan as containing blocked changes, so apply gates reject it
// before the apply starts.
func TestPlanResponse_HasBlockedChanges(t *testing.T) {
	tests := []struct {
		name string
		resp *PlanResponse
		want bool
	}{
		{
			name: "namespace-level blocked change",
			resp: &PlanResponse{
				Changes: []*SchemaChangeResponse{{
					Namespace: "testdb",
					TableChanges: []*TableChangeResponse{
						{TableName: "users", ExecutionMode: "blocked", ModeReason: "dropping primary key is not supported"},
					},
				}},
			},
			want: true,
		},
		{
			name: "per-shard blocked change only",
			resp: &PlanResponse{
				Shards: []*ShardPlanResponse{{
					Shard: "-40",
					Changes: []*TableChangeResponse{
						{TableName: "users", ExecutionMode: "blocked"},
					},
				}},
			},
			want: true,
		},
		{
			name: "direct and default modes are not blocked",
			resp: &PlanResponse{
				Changes: []*SchemaChangeResponse{{
					Namespace: "testdb",
					TableChanges: []*TableChangeResponse{
						{TableName: "users", ExecutionMode: "direct"},
						{TableName: "orders"},
					},
				}},
			},
			want: false,
		},
		{
			name: "nil plan",
			resp: nil,
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.resp.HasBlockedChanges())
		})
	}
}

// DirectChanges collects direct-execution verdicts across namespace-level and
// per-shard changes, and only those.
func TestPlanResponse_DirectChanges(t *testing.T) {
	resp := &PlanResponse{
		Changes: []*SchemaChangeResponse{{
			Namespace: "testdb",
			TableChanges: []*TableChangeResponse{
				{TableName: "users", ExecutionMode: "direct", ModeReason: "dropping primary key is not supported; runs as native MySQL DDL on a table with ~40 rows"},
				{TableName: "orders"},
				{TableName: "items", ExecutionMode: "blocked"},
			},
		}},
		Shards: []*ShardPlanResponse{{
			Shard: "-40",
			Changes: []*TableChangeResponse{
				{TableName: "mutes", ExecutionMode: "direct"},
			},
		}},
	}

	direct := resp.DirectChanges()
	require.Len(t, direct, 2)
	assert.Equal(t, "users", direct[0].TableName)
	assert.Equal(t, "mutes", direct[1].TableName)

	assert.Empty(t, (&PlanResponse{}).DirectChanges())
}

// AllChangesDirect holds only when the plan has at least one table change and
// every one carries the direct verdict — a mixed or empty plan still has
// engine-driven work, and a VSchema change is never direct.
func TestPlanResponse_AllChangesDirect(t *testing.T) {
	direct := func(table string) *TableChangeResponse {
		return &TableChangeResponse{TableName: table, ExecutionMode: "direct"}
	}
	tests := []struct {
		name string
		resp *PlanResponse
		want bool
	}{
		{
			name: "single direct change",
			resp: &PlanResponse{
				Changes: []*SchemaChangeResponse{{Namespace: "testdb", TableChanges: []*TableChangeResponse{direct("users")}}},
			},
			want: true,
		},
		{
			name: "mixed direct and engine-driven",
			resp: &PlanResponse{
				Changes: []*SchemaChangeResponse{{
					Namespace:    "testdb",
					TableChanges: []*TableChangeResponse{direct("users"), {TableName: "orders"}},
				}},
			},
			want: false,
		},
		{
			name: "direct namespace change with engine-driven shard change",
			resp: &PlanResponse{
				Changes: []*SchemaChangeResponse{{Namespace: "testdb", TableChanges: []*TableChangeResponse{direct("users")}}},
				Shards:  []*ShardPlanResponse{{Shard: "-40", Changes: []*TableChangeResponse{{TableName: "orders"}}}},
			},
			want: false,
		},
		{
			name: "vschema change alongside a direct change",
			resp: &PlanResponse{
				Changes: []*SchemaChangeResponse{{
					Namespace:    "testdb",
					TableChanges: []*TableChangeResponse{direct("users")},
					Metadata:     map[string]string{"vschema": "{}"},
				}},
			},
			want: false,
		},
		{
			name: "no changes",
			resp: &PlanResponse{},
			want: false,
		},
		{
			name: "nil plan",
			resp: nil,
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.resp.AllChangesDirect())
		})
	}
}

// DiscardedCopies selects the copies an apply destroys. The caller uses it to
// decide whether an operator must confirm first, so an adopted copy — where
// nothing is lost — is the only thing it leaves out.
func TestPlanResponseDiscardedCopies(t *testing.T) {
	resp := &PlanResponse{
		ExistingCopies: []*ExistingCopyResponse{
			{Namespace: "orders_ks", Disposition: ExistingCopyDiscard, Tables: []string{"orders"}},
			{Namespace: "products_ks", Disposition: ExistingCopyAdopt, Tables: []string{"products"}},
		},
	}

	discarded := resp.DiscardedCopies()

	require.Len(t, discarded, 1)
	assert.Equal(t, "orders_ks", discarded[0].Namespace)
}

// A disposition this build does not recognize counts as discarded: the
// confirmation exists to protect work already done, and an unreadable verdict
// is not a reason to skip it.
func TestPlanResponseDiscardedCopiesCountsUnknownDisposition(t *testing.T) {
	resp := &PlanResponse{
		ExistingCopies: []*ExistingCopyResponse{
			{Namespace: "orders_ks", Disposition: "recycle", Tables: []string{"orders"}},
		},
	}

	require.Len(t, resp.DiscardedCopies(), 1)
}

// A clean target has nothing to confirm, so the apply proceeds as it always
// has.
func TestPlanResponseDiscardedCopiesEmpty(t *testing.T) {
	assert.Empty(t, (&PlanResponse{}).DiscardedCopies())
	assert.Empty(t, (*PlanResponse)(nil).DiscardedCopies())
}
