package ddl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A VSchema edit is the same at the head when every table, vindex, and the
// keyspace's settings merge three ways: the head may carry tables the base
// added since the approval, and may drop the PR's own version of a table the
// base now declares the same way, but routes nothing differently from what
// the reviewer approved.
func TestSameVSchemaChange(t *testing.T) {
	const (
		base = `{
  "sharded": true,
  "vindexes": {"hash": {"type": "hash"}},
  "tables": {
    "orders": {"column_vindexes": [{"column": "customer_id", "name": "hash"}]}
  }
}`
		// approved is base with the PR's items table.
		approved = `{
  "sharded": true,
  "vindexes": {"hash": {"type": "hash"}},
  "tables": {
    "orders": {"column_vindexes": [{"column": "customer_id", "name": "hash"}]},
    "items": {"column_vindexes": [{"column": "order_id", "name": "hash"}]}
  }
}`
		// newerBase is base after the default branch added a customers table.
		newerBase = `{
  "sharded": true,
  "vindexes": {"hash": {"type": "hash"}},
  "tables": {
    "orders": {"column_vindexes": [{"column": "customer_id", "name": "hash"}]},
    "customers": {"column_vindexes": [{"column": "id", "name": "hash"}]}
  }
}`
		// combined is newerBase with the PR's items table.
		combined = `{
  "sharded": true,
  "vindexes": {"hash": {"type": "hash"}},
  "tables": {
    "orders": {"column_vindexes": [{"column": "customer_id", "name": "hash"}]},
    "customers": {"column_vindexes": [{"column": "id", "name": "hash"}]},
    "items": {"column_vindexes": [{"column": "order_id", "name": "hash"}]}
  }
}`
	)
	tests := []struct {
		name                                   string
		approvedBase, approved, headBase, head string
		wantSame                               bool
		wantReason                             string
	}{
		{
			name:         "a table the PR adds next to a table the base added",
			approvedBase: base, approved: approved, headBase: newerBase, head: combined,
			wantSame: true,
		},
		{
			name:         "the same document reformatted with its keys reordered",
			approvedBase: base, approved: approved, headBase: base,
			head: `{"tables":{"items":{"column_vindexes":[{"name":"hash","column":"order_id"}]},` +
				`"orders":{"column_vindexes":[{"name":"hash","column":"customer_id"}]}},` +
				`"vindexes":{"hash":{"type":"hash"}},"sharded":true}`,
			wantSame: true,
		},
		{
			name:         "a VSchema the PR adds",
			approvedBase: "", approved: approved, headBase: "", head: approved,
			wantSame: true,
		},
		{
			name:         "a table the base now declares itself the same way",
			approvedBase: base, approved: approved, headBase: combined, head: combined,
			wantSame: true,
		},
		{
			name:         "a head that routes the PR's table by another column",
			approvedBase: base, approved: approved, headBase: newerBase,
			head: `{
  "sharded": true,
  "vindexes": {"hash": {"type": "hash"}},
  "tables": {
    "orders": {"column_vindexes": [{"column": "customer_id", "name": "hash"}]},
    "customers": {"column_vindexes": [{"column": "id", "name": "hash"}]},
    "items": {"column_vindexes": [{"column": "id", "name": "hash"}]}
  }
}`,
			wantReason: "vschema: the head declares table items differently from the approved commit",
		},
		{
			name:         "a head that adds a table the reviewer did not approve",
			approvedBase: base, approved: approved, headBase: base,
			head: `{
  "sharded": true,
  "vindexes": {"hash": {"type": "hash"}},
  "tables": {
    "orders": {"column_vindexes": [{"column": "customer_id", "name": "hash"}]},
    "items": {"column_vindexes": [{"column": "order_id", "name": "hash"}]},
    "payments": {"column_vindexes": [{"column": "order_id", "name": "hash"}]}
  }
}`,
			wantReason: "vschema: the head declares table payments differently from the approved commit",
		},
		{
			name:         "a head that changes the keyspace settings",
			approvedBase: base, approved: approved, headBase: base,
			head: `{
  "sharded": true,
  "require_explicit_routing": true,
  "vindexes": {"hash": {"type": "hash"}},
  "tables": {
    "orders": {"column_vindexes": [{"column": "customer_id", "name": "hash"}]},
    "items": {"column_vindexes": [{"column": "order_id", "name": "hash"}]}
  }
}`,
			wantReason: "vschema: the head declares the keyspace settings differently from the approved commit",
		},
		{
			name:         "a head that restores a table's routing the base changed",
			approvedBase: base, approved: approved,
			headBase: `{
  "sharded": true,
  "vindexes": {"hash": {"type": "hash"}},
  "tables": {
    "orders": {"column_vindexes": [{"column": "id", "name": "hash"}]}
  }
}`,
			head:       approved,
			wantReason: "vschema: the base changed table orders since the approved commit, and the head replaces it with another version",
		},
		{
			name:         "a table the PR and the base both define differently",
			approvedBase: base, approved: approved,
			headBase: `{
  "sharded": true,
  "vindexes": {"hash": {"type": "hash"}},
  "tables": {
    "orders": {"column_vindexes": [{"column": "customer_id", "name": "hash"}]},
    "items": {"column_vindexes": [{"column": "id", "name": "hash"}]}
  }
}`,
			head:       approved,
			wantReason: "vschema: both the approved commit and the base changed table items",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			same, reason, err := SameVSchemaChange(tt.approvedBase, tt.approved, tt.headBase, tt.head)
			require.NoError(t, err)
			assert.Equal(t, tt.wantSame, same)
			assert.Equal(t, tt.wantReason, reason)
		})
	}
}

// A VSchema the vendored definition cannot read proves nothing, so the caller
// counts the file as changed.
func TestSameVSchemaChange_CannotCompare(t *testing.T) {
	const keyspace = `{"sharded": true, "vindexes": {"hash": {"type": "hash"}}}`
	tests := []struct {
		name    string
		head    string
		wantErr string
	}{
		{name: "malformed JSON", head: `{"sharded": true,`, wantErr: "read the VSchema at the head: parse VSchema"},
		{name: "a field Vitess does not define", head: `{"sharded": true, "shard_mode": "fast"}`, wantErr: "read the VSchema at the head: parse VSchema"},
		{name: "a table field Vitess does not define", head: `{"tables": {"orders": {"route_by": "id"}}}`, wantErr: "read the VSchema at the head: parse VSchema"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			same, reason, err := SameVSchemaChange(keyspace, keyspace, keyspace, tt.head)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
			assert.False(t, same)
			assert.Empty(t, reason)
		})
	}
}
