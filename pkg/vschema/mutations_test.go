package vschema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMutations_NoChange(t *testing.T) {
	mutations, err := Mutations(shardedVSchema, shardedVSchema)
	require.NoError(t, err)
	assert.Empty(t, mutations)
}

func TestMutations_EmptyCurrent(t *testing.T) {
	// A new keyspace has no current VSchema, so nothing can mutate.
	for _, current := range []string{"", "   ", "{}"} {
		mutations, err := Mutations(current, shardedVSchema)
		require.NoError(t, err)
		assert.Empty(t, mutations)
	}
}

func TestMutations_TypeChange(t *testing.T) {
	// Changing a vindex's type re-computes every row's keyspace id — an
	// effective re-shard of how queries route — so it must be disclosed even
	// though the vindex keeps its name.
	current := `{"sharded": true, "vindexes": {"user_idx": {"type": "hash"}}, "tables": {"users": {"column_vindexes": [{"column": "id", "name": "user_idx"}]}}}`
	desired := `{"sharded": true, "vindexes": {"user_idx": {"type": "xxhash"}}, "tables": {"users": {"column_vindexes": [{"column": "id", "name": "user_idx"}]}}}`
	mutations, err := Mutations(current, desired)
	require.NoError(t, err)
	require.Len(t, mutations, 1)
	assert.Equal(t, MutationKindVindexType, mutations[0].Kind)
	assert.Equal(t, "user_idx", mutations[0].Name)
	assert.Contains(t, mutations[0].Reason, `"hash"`)
	assert.Contains(t, mutations[0].Reason, `"xxhash"`)
	assert.Contains(t, mutations[0].Reason, "keyspace id")
}

func TestMutations_LookupBackingTableRepoint(t *testing.T) {
	// Repointing a lookup vindex's backing table makes Vitess read and write
	// lookup rows in a different table immediately; the old table goes stale.
	desired := `{
		"sharded": true,
		"vindexes": {
			"hash": {"type": "hash"},
			"email_lookup": {
				"type": "consistent_lookup_unique",
				"params": {"table": "email_lookup_v2", "from": "email", "to": "keyspace_id"},
				"owner": "users"
			}
		},
		"tables": {
			"users": {
				"column_vindexes": [
					{"column": "id", "name": "hash"},
					{"column": "email", "name": "email_lookup"}
				]
			},
			"orders": {
				"column_vindexes": [
					{"column": "user_id", "name": "hash"}
				]
			}
		}
	}`
	mutations, err := Mutations(shardedVSchema, desired)
	require.NoError(t, err)
	require.Len(t, mutations, 1)
	assert.Equal(t, MutationKindVindexParams, mutations[0].Kind)
	assert.Equal(t, "email_lookup", mutations[0].Name)
	assert.Contains(t, mutations[0].Reason, "repoints its backing table")
	assert.Contains(t, mutations[0].Reason, `"email_lookup"`)
	assert.Contains(t, mutations[0].Reason, `"email_lookup_v2"`)
}

func TestMutations_GenericParamChangeAddAndRemove(t *testing.T) {
	// Every changed, added, or removed param is reported individually, in
	// key order.
	current := `{"sharded": true, "vindexes": {"v": {"type": "region_json", "params": {"region_map": "/etc/a.json", "region_bytes": "1"}}}, "tables": {}}`
	desired := `{"sharded": true, "vindexes": {"v": {"type": "region_json", "params": {"region_map": "/etc/b.json", "extra": "x"}}}, "tables": {}}`
	mutations, err := Mutations(current, desired)
	require.NoError(t, err)
	require.Len(t, mutations, 3)
	for _, m := range mutations {
		assert.Equal(t, MutationKindVindexParams, m.Kind)
		assert.Equal(t, "v", m.Name)
	}
	assert.Contains(t, mutations[0].Reason, `"extra"`)
	assert.Contains(t, mutations[1].Reason, `"region_bytes"`)
	assert.Contains(t, mutations[2].Reason, `"region_map"`)
	assert.Contains(t, mutations[2].Reason, `"/etc/a.json"`)
	assert.Contains(t, mutations[2].Reason, `"/etc/b.json"`)
}

func TestMutations_OwnerChange(t *testing.T) {
	current := `{"sharded": true, "vindexes": {"l": {"type": "lookup", "params": {"table": "l", "from": "a", "to": "b"}, "owner": "users"}}, "tables": {}}`
	desired := `{"sharded": true, "vindexes": {"l": {"type": "lookup", "params": {"table": "l", "from": "a", "to": "b"}, "owner": "orders"}}, "tables": {}}`
	mutations, err := Mutations(current, desired)
	require.NoError(t, err)
	require.Len(t, mutations, 1)
	assert.Equal(t, MutationKindVindexOwner, mutations[0].Kind)
	assert.Equal(t, "l", mutations[0].Name)
	assert.Contains(t, mutations[0].Reason, `"users"`)
	assert.Contains(t, mutations[0].Reason, `"orders"`)
}

func TestMutations_RemovedVindexNotDuplicated(t *testing.T) {
	// A vindex that disappears entirely is a removal — Deletions reports it,
	// Mutations stays silent so the same change is not disclosed twice.
	current := `{"sharded": true, "vindexes": {"gone": {"type": "hash"}}, "tables": {}}`
	desired := `{"sharded": true, "vindexes": {}, "tables": {}}`
	mutations, err := Mutations(current, desired)
	require.NoError(t, err)
	assert.Empty(t, mutations)
}

func TestMutations_CombinedTypeParamsOwner(t *testing.T) {
	// A vindex changing several aspects at once reports each one, so the
	// operator sees the full blast radius: type, then params, then owner.
	current := `{"sharded": true, "vindexes": {"l": {"type": "lookup_unique", "params": {"table": "l1", "from": "a", "to": "b"}, "owner": "users"}}, "tables": {}}`
	desired := `{"sharded": true, "vindexes": {"l": {"type": "consistent_lookup_unique", "params": {"table": "l2", "from": "a", "to": "b"}, "owner": "orders"}}, "tables": {}}`
	mutations, err := Mutations(current, desired)
	require.NoError(t, err)
	require.Len(t, mutations, 3)
	assert.Equal(t, MutationKindVindexType, mutations[0].Kind)
	assert.Equal(t, MutationKindVindexParams, mutations[1].Kind)
	assert.Contains(t, mutations[1].Reason, "repoints its backing table")
	assert.Equal(t, MutationKindVindexOwner, mutations[2].Kind)
}

func TestMutations_UnparseableCurrentFailsClosed(t *testing.T) {
	_, err := Mutations("{not json", shardedVSchema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse current VSchema")
}

func TestMutations_UnparseableDesiredFailsClosed(t *testing.T) {
	_, err := Mutations(shardedVSchema, "{not json")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse desired VSchema")
}
